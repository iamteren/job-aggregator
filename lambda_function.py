import json
import urllib.request
import urllib.error
import urllib.parse
import boto3
import os
import hashlib
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET        = os.environ["S3_BUCKET"]
DYNAMO_TABLE     = os.environ["DYNAMO_TABLE"]
JOBS_TABLE       = os.environ.get("JOBS_TABLE", "")
SNS_TOPIC_ARN    = os.environ.get("SNS_TOPIC_ARN", "")
TTL_DAYS         = 30
CATALOG_TTL_DAYS = int(os.environ.get("CATALOG_TTL_DAYS", 14))

s3     = boto3.client("s3")
dynamo = boto3.resource("dynamodb")
ssm    = boto3.client("ssm")
sns    = boto3.client("sns")

# ---------------------------------------------------------------------------
# Cold-start secret loader
# ---------------------------------------------------------------------------
def load_secrets():
    try:
        resp = ssm.get_parameters_by_path(
            Path="/job-fetcher/",
            WithDecryption=True,
        )
        params = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
        return {
            "usajobs_api_key":       params.get("usajobs-api-key", ""),
            "usajobs_email":         params.get("usajobs-email", ""),
            "careeronestop_user_id": params.get("careeronestop-user-id", ""),
            "careeronestop_token":   params.get("careeronestop-token", ""),
        }
    except Exception as e:
        print(f"[WARN] Could not load SSM secrets: {e}")
        return {}

SECRETS = load_secrets()

# ---------------------------------------------------------------------------
# Sources config — unauthenticated
# ---------------------------------------------------------------------------
SOURCES = {
    "remoteok": {
        "url": "https://remoteok.com/api",
        "headers": {"User-Agent": "job-aggregator-bot/1.0"},
    },
    "remotive": {
        "url": "https://remotive.com/api/remote-jobs",
        "headers": {"User-Agent": "job-aggregator-bot/1.0"},
    },
    "arbeitnow": {
        "url": "https://www.arbeitnow.com/api/job-board-api",
        "headers": {"User-Agent": "job-aggregator-bot/1.0"},
    },
}

# ---------------------------------------------------------------------------
# Dedup helpers
# ---------------------------------------------------------------------------
def make_job_id(source, job):
    if source == "remoteok":
        raw = str(job.get("id") or job.get("url") or job.get("position", ""))
    elif source == "remotive":
        raw = str(job.get("id") or job.get("url") or job.get("title", ""))
    elif source == "arbeitnow":
        raw = str(job.get("slug") or job.get("url") or job.get("title", ""))
    elif source == "usajobs":
        raw = str(
            job.get("MatchedObjectId")
            or job.get("PositionID")
            or job.get("PositionTitle", "")
        )
    elif source == "careeronestop":
        raw = str(job.get("JobID") or job.get("JobTitle", "") + job.get("Company", ""))
    else:
        raw = json.dumps(job, sort_keys=True)

    if len(raw) > 64:
        raw = hashlib.md5(raw.encode()).hexdigest()

    return f"{source}#{raw}"


def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def filter_seen_jobs(source, jobs, table):
    if not jobs:
        return [], []

    job_ids = [make_job_id(source, j) for j in jobs]
    seen = set()

    for id_chunk in chunk(job_ids, 100):
        keys = [{"job_id": jid} for jid in id_chunk]
        resp = dynamo.batch_get_item(
            RequestItems={DYNAMO_TABLE: {"Keys": keys, "ProjectionExpression": "job_id"}}
        )
        for item in resp.get("Responses", {}).get(DYNAMO_TABLE, []):
            seen.add(item["job_id"])

    new_jobs, new_ids = [], []
    for job, jid in zip(jobs, job_ids):
        if jid not in seen:
            new_jobs.append(job)
            new_ids.append(jid)

    return new_jobs, new_ids


def mark_jobs_seen(job_ids, now_epoch, table):
    if not job_ids:
        return

    expires_at = now_epoch + (TTL_DAYS * 86400)

    for id_chunk in chunk(job_ids, 25):
        requests = [
            {"PutRequest": {"Item": {"job_id": jid, "expires_at": expires_at}}}
            for jid in id_chunk
        ]
        dynamo.batch_write_item(RequestItems={DYNAMO_TABLE: requests})


# ---------------------------------------------------------------------------
# Job catalog helpers (JOBS_TABLE)
# ---------------------------------------------------------------------------
def _parse_posted(posted_val, fallback_date):
    """Normalize any posted value to YYYY-MM-DD. Falls back to fetched date."""
    if not posted_val:
        return fallback_date
    s = str(posted_val).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        epoch = int(s)
        return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")
    except ValueError:
        pass
    return fallback_date


def write_jobs_catalog(source, new_jobs, now_epoch, catalog_ttl_days):
    """Write normalized new jobs to JOBS_TABLE with TTL."""
    jobs_table_name = os.environ.get("JOBS_TABLE", "")
    if not jobs_table_name or not new_jobs:
        return
    jobs_table    = dynamo.Table(jobs_table_name)
    fallback_date = datetime.fromtimestamp(now_epoch, tz=timezone.utc).strftime("%Y-%m-%d")
    expires_at    = now_epoch + (catalog_ttl_days * 86400)

    for job_chunk in chunk(new_jobs, 25):
        requests = []
        for job in job_chunk:
            n      = _normalize(source, job)
            posted = _parse_posted(n.get("posted"), fallback_date)
            jid    = make_job_id(source, job)
            requests.append({
                "PutRequest": {
                    "Item": {
                        "job_id":      jid,
                        "source":      source,
                        "posted":      posted,
                        "expires_at":  expires_at,
                        "title":       n.get("title", ""),
                        "company":     n.get("company", ""),
                        "location":    n.get("location", ""),
                        "remote":      n.get("remote", ""),
                        "salary":      n.get("salary", ""),
                        "tags":        n.get("tags", ""),
                        "url":         n.get("url", ""),
                        "description": n.get("description", ""),
                    }
                }
            })
        dynamo.batch_write_item(RequestItems={jobs_table_name: requests})


def query_catalog(catalog_ttl_days):
    """Scan JOBS_TABLE for jobs posted within the catalog window."""
    jobs_table_name = os.environ.get("JOBS_TABLE", "")
    if not jobs_table_name:
        return {}
    jobs_table = dynamo.Table(jobs_table_name)
    from boto3.dynamodb.conditions import Attr
    cutoff_epoch = int(datetime.now(timezone.utc).timestamp()) - (catalog_ttl_days * 86400)
    cutoff_date  = datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc).strftime("%Y-%m-%d")

    jobs_by_source = {}
    kwargs = {
        "FilterExpression": Attr("posted").gte(cutoff_date),
        "ProjectionExpression":
            "job_id, #src, posted, title, company, #loc, remote, salary, tags, #url, description",
        "ExpressionAttributeNames": {"#loc": "location", "#url": "url", "#src": "source"},
    }
    while True:
        resp = jobs_table.scan(**kwargs)
        for item in resp.get("Items", []):
            src = item.get("source", "unknown")
            jobs_by_source.setdefault(src, [])
            jobs_by_source[src].append(item)
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    for src in jobs_by_source:
        jobs_by_source[src].sort(key=lambda j: j.get("posted", ""), reverse=True)

    return jobs_by_source

# ---------------------------------------------------------------------------
# Fetchers — unauthenticated
# ---------------------------------------------------------------------------
def fetch_source(name, config):
    try:
        req = urllib.request.Request(config["url"], headers=config["headers"])
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        if name == "remoteok":
            jobs = [j for j in raw if isinstance(j, dict) and j.get("position")]
        elif name == "remotive":
            jobs = raw.get("jobs", [])
        elif name == "arbeitnow":
            US_KEYWORDS = ["United States", "USA", " US", "Remote", "Worldwide", "remote", "worldwide"]
            jobs = [
                j for j in raw.get("data", [])
                if j.get("remote") is True
                or any(kw in (j.get("location") or "") for kw in US_KEYWORDS)
            ]
        else:
            jobs = raw if isinstance(raw, list) else []

        return name, jobs, None

    except urllib.error.HTTPError as e:
        return name, [], f"HTTPError {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return name, [], f"URLError: {e.reason}"
    except json.JSONDecodeError as e:
        return name, [], f"JSONDecodeError: {e.msg}"
    except Exception as e:
        return name, [], f"UnexpectedError: {str(e)}"


# ---------------------------------------------------------------------------
# Fetcher — USAJOBS
# ---------------------------------------------------------------------------
def fetch_usajobs():
    name    = "usajobs"
    api_key = SECRETS.get("usajobs_api_key", "")
    email   = SECRETS.get("usajobs_email", "")

    if not api_key or not email:
        return name, [], "Missing USAJOBS credentials in SSM"

    url = "https://data.usajobs.gov/api/search?ResultsPerPage=100&Page=1"
    headers = {
        "Authorization-Key": api_key,
        "User-Agent":        email,
        "Host":              "data.usajobs.gov",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        jobs = raw.get("SearchResult", {}).get("SearchResultItems", [])
        jobs = [j.get("MatchedObjectDescriptor", j) for j in jobs]
        return name, jobs, None

    except urllib.error.HTTPError as e:
        return name, [], f"HTTPError {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return name, [], f"URLError: {e.reason}"
    except json.JSONDecodeError as e:
        return name, [], f"JSONDecodeError: {e.msg}"
    except Exception as e:
        return name, [], f"UnexpectedError: {str(e)}"


# ---------------------------------------------------------------------------
# Fetcher — CareerOneStop
# ---------------------------------------------------------------------------
def fetch_careeronestop():
    name    = "careeronestop"
    user_id = SECRETS.get("careeronestop_user_id", "")
    token   = SECRETS.get("careeronestop_token", "")

    if not user_id or not token:
        return name, [], "Missing CareerOneStop credentials in SSM"

    keyword  = urllib.parse.quote("software")
    location = urllib.parse.quote("United States")
    url = (
        f"https://api.careeronestop.org/v1/jobsearch/{user_id}"
        f"/{keyword}/{location}/0/0/0/0/1/100"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        jobs = raw.get("Jobs", [])
        return name, jobs, None

    except urllib.error.HTTPError as e:
        return name, [], f"HTTPError {e.code}: {e.reason}"
    except urllib.error.URLError as e:
        return name, [], f"URLError: {e.reason}"
    except json.JSONDecodeError as e:
        return name, [], f"JSONDecodeError: {e.msg}"
    except Exception as e:
        return name, [], f"UnexpectedError: {str(e)}"


# ---------------------------------------------------------------------------
# S3 writer — skips write if no new jobs and no error
# ---------------------------------------------------------------------------
def save_to_s3(name, jobs, fetched_at, fetched_total, error):
    if not jobs and not error:
        return None

    key = f"{name}/{fetched_at}.json"
    payload = {
        "source":        name,
        "fetched_at":    fetched_at,
        "fetched_total": fetched_total,
        "new_jobs":      len(jobs),
        "error":         error,
        "jobs":          jobs,
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, default=str),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# HTML report generator
# ---------------------------------------------------------------------------
def _job_attr(source, job, field_map):
    for key in field_map:
        val = job.get(key)
        if val:
            if isinstance(val, list):
                return ", ".join(str(v) for v in val)
            return str(val)
    return ""


def _normalize(source, job):
    """Return a display-ready dict from any source's raw job object."""
    if source == "remoteok":
        return {
            "title":       job.get("position", ""),
            "company":     job.get("company", ""),
            "location":    job.get("location", "Worldwide"),
            "remote":      "Yes",
            "salary":      job.get("salary", ""),
            "tags":        ", ".join(job.get("tags", [])),
            "posted":      job.get("date", ""),
            "url":         job.get("url", ""),
            "description": job.get("description", ""),
        }
    elif source == "remotive":
        return {
            "title":       job.get("title", ""),
            "company":     job.get("company_name", ""),
            "location":    job.get("candidate_required_location", "Remote"),
            "remote":      "Yes",
            "salary":      job.get("salary", ""),
            "tags":        ", ".join(job.get("tags", [])),
            "posted":      job.get("published_date", ""),
            "url":         job.get("url", ""),
            "description": job.get("description", ""),
        }
    elif source == "arbeitnow":
        return {
            "title":       job.get("title", ""),
            "company":     job.get("company_name", ""),
            "location":    job.get("location", ""),
            "remote":      "Yes" if job.get("remote") else "No",
            "salary":      "",
            "tags":        ", ".join(job.get("tags", [])),
            "posted":      job.get("created_at", ""),
            "url":         job.get("url", ""),
            "description": job.get("description", ""),
        }
    elif source == "usajobs":
        remun = job.get("PositionRemuneration", [{}])
        salary = ""
        if remun:
            r = remun[0]
            salary = f"{r.get('MinimumRange','')}-{r.get('MaximumRange','')} {r.get('RateIntervalCode','')}".strip("- ")
        cats = job.get("JobCategory", [{}])
        tags = ", ".join(c.get("Name", "") for c in cats)
        locs = job.get("PositionLocation", [{}])
        location = ", ".join(l.get("LocationName", "") for l in locs)
        tele = job.get("TelecommutingCode", "")
        remote = "Yes" if tele and tele != "0" else "No"
        summary = ""
        try:
            summary = job.get("UserArea", {}).get("Details", {}).get("JobSummary", "")
        except Exception:
            pass
        return {
            "title":       job.get("PositionTitle", ""),
            "company":     job.get("OrganizationName", ""),
            "location":    location,
            "remote":      remote,
            "salary":      salary,
            "tags":        tags,
            "posted":      job.get("PublicationStartDate", "")[:10] if job.get("PublicationStartDate") else "",
            "url":         job.get("PositionURI", ""),
            "description": summary,
        }
    elif source == "careeronestop":
        return {
            "title":       job.get("JobTitle", ""),
            "company":     job.get("Company", ""),
            "location":    job.get("Location", ""),
            "remote":      "",
            "salary":      job.get("Pay", ""),
            "tags":        ", ".join(job.get("OnetTitles", [])),
            "posted":      job.get("DatePosted", ""),
            "url":         job.get("JobURL", ""),
            "description": job.get("JobDescription", ""),
        }
    return {k: "" for k in ["title","company","location","remote","salary","tags","posted","url","description"]}


def generate_html_report(jobs_by_source, run_meta, pre_normalized=False):
    fetched_at   = run_meta["fetched_at"]
    total_new    = run_meta["total_new"]
    total_fetched= run_meta["total_fetched"]
    results      = run_meta["results"]

    display_time = fetched_at.replace("T", " ").replace("-", ":", 2).replace("-", ":")

    # ── source summary rows ──────────────────────────────────────────────────
    source_rows = ""
    for src, r in sorted(results.items()):
        if r["error"]:
            badge  = f'<span class="badge badge-error">ERROR</span>'
            detail = f'<span class="err-text">{r["error"]}</span>'
        else:
            badge  = f'<span class="badge badge-ok">OK</span>'
            detail = f'{r["new_jobs"]} new &nbsp;/&nbsp; {r["fetched_total"]} fetched'
        source_rows += f"""
        <tr>
          <td class="src-name">{src}</td>
          <td>{badge}</td>
          <td>{detail}</td>
        </tr>"""

    # ── job cards ────────────────────────────────────────────────────────────
    all_cards  = ""
    tab_counts = {}
    job_index  = 0

    for src in sorted(jobs_by_source.keys()):
        jobs = jobs_by_source[src]
        tab_counts[src] = len(jobs)
        for job in jobs:
            n    = job if pre_normalized else _normalize(src, job)
            idx  = job_index
            job_index += 1

            title    = n["title"]   or "Untitled"
            company  = n["company"] or "Unknown"
            location = n["location"] or "—"
            remote   = n["remote"]
            salary   = n["salary"]
            tags     = n["tags"]
            posted   = str(n["posted"])[:10] if n["posted"] else ""
            url      = n["url"]
            desc_raw = n["description"] or ""
            # strip basic html tags then convert markdown to html
            import re as _re
            # preserve newlines from block-level tags before stripping
            desc_clean = _re.sub(r'<br\s*/?>', '\n', desc_raw, flags=_re.IGNORECASE)
            desc_clean = _re.sub(r'</(p|div|li|h[1-6])>', '\n', desc_clean, flags=_re.IGNORECASE)
            desc_clean = _re.sub(r'<[^>]+>', '', desc_clean).strip()
            # convert markdown to html
            def md_to_html(text):
                # headers
                text = _re.sub(r'^### (.+)$', r'<h4>\1</h4>', text, flags=_re.MULTILINE)
                text = _re.sub(r'^## (.+)$',  r'<h3>\1</h3>', text, flags=_re.MULTILINE)
                text = _re.sub(r'^# (.+)$',   r'<h2>\1</h2>', text, flags=_re.MULTILINE)
                # bold / italic
                text = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
                text = _re.sub(r'\*(.+?)\*',     r'<em>\1</em>', text)
                # bullet lists
                text = _re.sub(r'(?m)^[\*\-] (.+)$', r'<li>\1</li>', text)
                text = _re.sub(r'(<li>.*?</li>)', r'<ul>\1</ul>', text, flags=_re.DOTALL)
                text = _re.sub(r'</ul>\s*<ul>', '', text)
                # line breaks
                text = _re.sub(r'\n{2,}', '</p><p>', text)
                text = text.replace('\n', '<br>')
                return f'<p>{text}</p>'
            desc_html    = md_to_html(desc_clean)
            desc_preview = desc_clean[:280] + ("…" if len(desc_clean) > 280 else "")
            desc_full    = desc_html

            remote_badge = '<span class="tag tag-remote">Remote</span>' if remote == "Yes" else ""
            salary_html  = f'<span class="tag tag-salary">{salary}</span>' if salary else ""
            tags_html    = "".join(f'<span class="tag">{t.strip()}</span>' for t in tags.split(",") if t.strip())
            view_btn     = f'<a href="{url}" target="_blank" class="view-btn">View Posting →</a>' if url else ""

            all_cards += f"""
        <div class="job-card" data-source="{src}" data-idx="{idx}">
          <div class="job-header" onclick="toggle({idx})">
            <div class="job-title-wrap">
              <span class="chevron" id="chev-{idx}">▶</span>
              <span class="job-title">{title}</span>
            </div>
            <div class="job-meta-inline">
              <span class="company">{company}</span>
              <span class="location">📍 {location}</span>
              {remote_badge}
              <span class="source-pill">{src}</span>
              <span class="posted-date">{posted}</span>
            </div>
          </div>
          <div class="job-body" id="body-{idx}">
            <div class="detail-grid">
              <div class="detail-row"><span class="dl">Company</span><span class="dv">{company}</span></div>
              <div class="detail-row"><span class="dl">Location</span><span class="dv">{location}</span></div>
              {'<div class="detail-row"><span class="dl">Remote</span><span class="dv">' + remote + '</span></div>' if remote else ''}
              {'<div class="detail-row"><span class="dl">Salary</span><span class="dv">' + salary + '</span></div>' if salary else ''}
              {'<div class="detail-row"><span class="dl">Posted</span><span class="dv">' + posted + '</span></div>' if posted else ''}
              {'<div class="detail-row"><span class="dl">Tags</span><span class="dv">' + tags_html + '</span></div>' if tags else ''}
            </div>
            <div class="desc-preview" id="desc-short-{idx}">{desc_preview}</div>
            <div class="desc-full hidden" id="desc-full-{idx}">{desc_full}</div>
            {'<button class="toggle-desc" onclick="toggleDesc(' + str(idx) + ')">Show more</button>' if len(desc_clean) > 280 else ''}
            <div class="card-footer">{view_btn}</div>
          </div>
        </div>"""

    # ── tab buttons ──────────────────────────────────────────────────────────
    tab_btns = '<button class="tab-btn active" onclick="filterSource(\'all\', this)">All <span class="tab-count">' + str(total_new) + '</span></button>'
    for src in sorted(tab_counts.keys()):
        cnt = tab_counts[src]
        tab_btns += f'<button class="tab-btn" onclick="filterSource(\'{src}\', this)">{src} <span class="tab-count">{cnt}</span></button>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Job Report — {display_time}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
          background: #f4f6f9; color: #1a1a2e; min-height: 100vh; }}
  /* ── header ── */
  .header {{ background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
             color: #fff; padding: 32px 40px 24px; }}
  .header h1 {{ font-size: 1.6rem; font-weight: 700; letter-spacing: -0.3px; }}
  .header .subtitle {{ color: #a0aec0; font-size: 0.85rem; margin-top: 4px; }}
  .stats-bar {{ display: flex; gap: 24px; margin-top: 20px; flex-wrap: wrap; }}
  .stat {{ background: rgba(255,255,255,0.08); border-radius: 10px;
           padding: 12px 20px; min-width: 110px; }}
  .stat .val {{ font-size: 1.8rem; font-weight: 700; color: #63b3ed; }}
  .stat .lbl {{ font-size: 0.72rem; color: #a0aec0; text-transform: uppercase;
                letter-spacing: 0.5px; margin-top: 2px; }}
  /* ── source table ── */
  .source-section {{ background: #fff; margin: 24px 40px 0;
                     border-radius: 12px; overflow: hidden;
                     box-shadow: 0 1px 4px rgba(0,0,0,0.08); }}
  .source-section table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
  .source-section th {{ background: #f7fafc; color: #718096; font-weight: 600;
                        text-transform: uppercase; font-size: 0.72rem;
                        letter-spacing: 0.5px; padding: 10px 16px; text-align: left; }}
  .source-section td {{ padding: 10px 16px; border-top: 1px solid #edf2f7; }}
  .src-name {{ font-weight: 600; color: #2d3748; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 20px;
            font-size: 0.72rem; font-weight: 700; }}
  .badge-ok    {{ background: #c6f6d5; color: #276749; }}
  .badge-error {{ background: #fed7d7; color: #9b2c2c; }}
  .err-text {{ color: #e53e3e; font-size: 0.82rem; }}
  /* ── toolbar ── */
  .toolbar {{ margin: 20px 40px 0; display: flex; gap: 12px;
              align-items: center; flex-wrap: wrap; }}
  .search-box {{ flex: 1; min-width: 200px; padding: 9px 14px;
                 border: 1px solid #e2e8f0; border-radius: 8px;
                 font-size: 0.9rem; outline: none; background: #fff; }}
  .search-box:focus {{ border-color: #63b3ed; box-shadow: 0 0 0 3px rgba(99,179,237,0.2); }}
  .tabs {{ display: flex; gap: 6px; flex-wrap: wrap; }}
  .tab-btn {{ padding: 7px 14px; border: 1px solid #e2e8f0; border-radius: 20px;
              background: #fff; font-size: 0.82rem; cursor: pointer;
              color: #4a5568; transition: all 0.15s; }}
  .tab-btn:hover {{ background: #ebf8ff; border-color: #90cdf4; }}
  .tab-btn.active {{ background: #2b6cb0; color: #fff; border-color: #2b6cb0; }}
  .tab-count {{ background: rgba(255,255,255,0.25); border-radius: 10px;
                padding: 1px 6px; font-size: 0.72rem; margin-left: 4px; }}
  .tab-btn:not(.active) .tab-count {{ background: #edf2f7; color: #718096; }}
  /* ── job list ── */
  .job-list {{ margin: 16px 40px 40px; display: flex; flex-direction: column; gap: 8px; }}
  .job-card {{ background: #fff; border-radius: 10px;
               box-shadow: 0 1px 3px rgba(0,0,0,0.07);
               border: 1px solid #e8edf3; overflow: hidden;
               transition: box-shadow 0.15s; }}
  .job-card:hover {{ box-shadow: 0 4px 12px rgba(0,0,0,0.1); }}
  .job-header {{ padding: 14px 18px; cursor: pointer; display: flex;
                 align-items: center; justify-content: space-between;
                 gap: 12px; flex-wrap: wrap; }}
  .job-title-wrap {{ display: flex; align-items: center; gap: 8px; flex: 1; min-width: 200px; }}
  .chevron {{ color: #a0aec0; font-size: 0.7rem; transition: transform 0.2s; user-select: none; }}
  .chevron.open {{ transform: rotate(90deg); color: #2b6cb0; }}
  .job-title {{ font-weight: 600; font-size: 0.95rem; color: #1a202c; }}
  .job-meta-inline {{ display: flex; align-items: center; gap: 10px;
                      flex-wrap: wrap; font-size: 0.82rem; color: #718096; }}
  .company {{ font-weight: 500; color: #2d3748; }}
  .location {{ color: #718096; }}
  .source-pill {{ background: #ebf8ff; color: #2b6cb0; border-radius: 20px;
                  padding: 2px 9px; font-size: 0.72rem; font-weight: 600; }}
  .posted-date {{ color: #a0aec0; font-size: 0.78rem; }}
  .tag {{ display: inline-block; background: #f7fafc; border: 1px solid #e2e8f0;
          border-radius: 4px; padding: 1px 7px; font-size: 0.72rem;
          color: #4a5568; margin: 1px; }}
  .tag-remote {{ background: #c6f6d5; border-color: #9ae6b4; color: #276749; }}
  .tag-salary {{ background: #fefcbf; border-color: #f6e05e; color: #744210; }}
  /* ── expanded body ── */
  .job-body {{ display: none; padding: 0 18px 16px; border-top: 1px solid #edf2f7; }}
  .job-body.open {{ display: block; }}
  .detail-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
                  gap: 6px; margin: 12px 0; }}
  .detail-row {{ display: flex; gap: 8px; font-size: 0.85rem; }}
  .dl {{ color: #718096; min-width: 70px; font-weight: 500; }}
  .dv {{ color: #2d3748; }}
  .desc-preview, .desc-full {{ font-size: 0.85rem; color: #4a5568; line-height: 1.6;
                                margin: 10px 0 6px; }}
  .hidden {{ display: none; }}
  .toggle-desc {{ background: none; border: none; color: #2b6cb0; font-size: 0.82rem;
                  cursor: pointer; padding: 0; margin-bottom: 10px; }}
  .toggle-desc:hover {{ text-decoration: underline; }}
  .card-footer {{ margin-top: 10px; display: flex; justify-content: flex-end; }}
  .view-btn {{ display: inline-block; background: #2b6cb0; color: #fff;
               padding: 8px 18px; border-radius: 7px; font-size: 0.85rem;
               font-weight: 600; text-decoration: none; transition: background 0.15s; }}
  .view-btn:hover {{ background: #2c5282; }}
  /* ── empty state ── */
  .empty {{ text-align: center; padding: 60px 20px; color: #a0aec0; font-size: 0.95rem; }}
  /* ── responsive ── */
  @media (max-width: 640px) {{
    .header, .source-section, .toolbar, .job-list {{ margin-left: 16px; margin-right: 16px; }}
    .stats-bar {{ gap: 12px; }}
  }}
</style>
</head>
<body>

<div class="header">
  <h1>Job Aggregator Report</h1>
  <div class="subtitle">Run at {display_time} UTC</div>
  <div class="stats-bar">
    <div class="stat"><div class="val">{total_new}</div><div class="lbl">New Jobs</div></div>
    <div class="stat"><div class="val">{total_fetched}</div><div class="lbl">Total Fetched</div></div>
    <div class="stat"><div class="val">{total_fetched - total_new}</div><div class="lbl">Duplicates</div></div>
    <div class="stat"><div class="val">{len(results)}</div><div class="lbl">Sources</div></div>
  </div>
</div>

<div class="source-section">
  <table>
    <thead><tr><th>Source</th><th>Status</th><th>Detail</th></tr></thead>
    <tbody>{source_rows}</tbody>
  </table>
</div>

<div class="toolbar">
  <input class="search-box" type="text" placeholder="Search jobs, companies, tags…" oninput="filterSearch(this.value)"/>
  <div class="tabs">{tab_btns}</div>
</div>

<div class="job-list" id="job-list">
{all_cards}
</div>

<script>
  var activeSource = 'all';
  var activeSearch = '';

  function toggle(idx) {{
    var body  = document.getElementById('body-' + idx);
    var chev  = document.getElementById('chev-' + idx);
    var open  = body.classList.toggle('open');
    chev.classList.toggle('open', open);
  }}

  function toggleDesc(idx) {{
    var s = document.getElementById('desc-short-' + idx);
    var f = document.getElementById('desc-full-'  + idx);
    var btn = s.nextElementSibling && s.nextElementSibling.tagName === 'BUTTON'
              ? s.nextElementSibling : f.nextElementSibling;
    if (f.classList.contains('hidden')) {{
      s.classList.add('hidden'); f.classList.remove('hidden');
      if (btn) btn.textContent = 'Show less';
    }} else {{
      f.classList.add('hidden'); s.classList.remove('hidden');
      if (btn) btn.textContent = 'Show more';
    }}
  }}

  function applyFilters() {{
    var cards = document.querySelectorAll('.job-card');
    var shown = 0;
    cards.forEach(function(c) {{
      var srcMatch = activeSource === 'all' || c.dataset.source === activeSource;
      var txt      = c.textContent.toLowerCase();
      var srchMatch= activeSearch === '' || txt.indexOf(activeSearch) !== -1;
      var visible  = srcMatch && srchMatch;
      c.style.display = visible ? '' : 'none';
      if (visible) shown++;
    }});
    var empty = document.getElementById('empty-state');
    if (empty) empty.style.display = shown === 0 ? '' : 'none';
  }}

  function filterSource(src, btn) {{
    activeSource = src;
    document.querySelectorAll('.tab-btn').forEach(function(b) {{ b.classList.remove('active'); }});
    btn.classList.add('active');
    applyFilters();
  }}

  function filterSearch(val) {{
    activeSearch = val.toLowerCase().trim();
    applyFilters();
  }}
</script>

</body>
</html>"""
    return html


def save_report_to_s3(html, fetched_at):
    key = f"reports/{fetched_at.replace(':', '-').replace(' ', 'T')}.html"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )
    url = f"https://{S3_BUCKET}.s3.amazonaws.com/{key}"
    print(f"[REPORT] Uploaded -> {url}")
    return url

# ---------------------------------------------------------------------------
# SNS alert
# ---------------------------------------------------------------------------
def publish_alert(fetched_at, total_fetched, total_new, results, report_url=""):
    if not SNS_TOPIC_ARN or total_new == 0:
        return

    errors = [n for n, r in results.items() if r["error"]]

    lines = [
        f"Job Aggregator Run - {fetched_at.replace('T', ' ').replace('-', ':', 2).replace('-', ':')}",
        f"",
        f"Total fetched : {total_fetched}",
        f"New jobs      : {total_new}",
        f"Dupes skipped : {total_fetched - total_new}",
        f"",
        f"Per source:",
    ]
    for name, r in sorted(results.items()):
        status = f"ERROR: {r['error']}" if r["error"] else f"{r['new_jobs']} new / {r['fetched_total']} fetched"
        lines.append(f"  {name:<20} {status}")

    if errors:
        lines += ["", f"Errors: {', '.join(errors)}"]

    if report_url:
        lines += [
            "",
            "── Full Report ──────────────────────────────",
            f"{report_url}",
            "─────────────────────────────────────────────",
        ]

    lines += [
        "",
        f"S3 bucket: s3://{S3_BUCKET}/",
        f"DynamoDB table: {DYNAMO_TABLE}",
    ]

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"[Job Aggregator] {total_new} new jobs found - {fetched_at}",
        Message="\n".join(lines),
    )
    print(f"[SNS] Alert published - {total_new} new jobs")

def lambda_handler(event, context):
    table      = dynamo.Table(DYNAMO_TABLE)
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    now_epoch  = int(datetime.now(timezone.utc).timestamp())
    results    = {}

    def run_source(name, config=None):
        if config:
            return fetch_source(name, config)
        elif name == "usajobs":
            return fetch_usajobs()
        elif name == "careeronestop":
            return fetch_careeronestop()

    tasks = {**{n: c for n, c in SOURCES.items()}, "usajobs": None, "careeronestop": None}
    catalog_ttl = int(event.get("catalog_days", os.environ.get("CATALOG_TTL_DAYS", 14)))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(run_source, name, config): name
            for name, config in tasks.items()
        }
        for future in as_completed(futures):
            name, jobs, error = future.result()
            fetched_total = len(jobs)

            new_jobs, new_ids = filter_seen_jobs(name, jobs, table)
            mark_jobs_seen(new_ids, now_epoch, table)
            write_jobs_catalog(name, new_jobs, now_epoch, catalog_ttl)

            s3_key = save_to_s3(name, new_jobs, fetched_at, fetched_total, error)

            results[name] = {
                "fetched_total":   fetched_total,
                "new_jobs":        len(new_jobs),
                "duplicate_jobs":  fetched_total - len(new_jobs),
                "s3_key":          s3_key,
                "error":           error,
            }

            if error:
                print(f"[{name}] ERROR: {error}")
            elif s3_key:
                print(f"[{name}] OK: {fetched_total} fetched, {len(new_jobs)} new, "
                      f"{fetched_total - len(new_jobs)} dupes -> s3://{S3_BUCKET}/{s3_key}")
            else:
                print(f"[{name}] {fetched_total} fetched, all dupes — S3 write skipped")

    total_new     = sum(r["new_jobs"]      for r in results.values())
    total_fetched = sum(r["fetched_total"] for r in results.values())
    files_written = sum(1 for r in results.values() if r["s3_key"])

    print(f"[DONE] Fetched: {total_fetched} | New: {total_new} | "
          f"Dupes skipped: {total_fetched - total_new} | Files written: {files_written}")

    report_url = ""
    jobs_by_source = query_catalog(catalog_ttl)
    total_catalog  = sum(len(v) for v in jobs_by_source.values())
    if total_catalog > 0 or total_new > 0:
        run_meta = {
            "fetched_at":    fetched_at,
            "total_new":     total_new,
            "total_fetched": total_fetched,
            "results":       results,
        }
        html       = generate_html_report(jobs_by_source, run_meta, pre_normalized=True)
        report_url = save_report_to_s3(html, fetched_at)

    publish_alert(fetched_at, total_fetched, total_new, results, report_url)

    return {
        "statusCode":       200,
        "fetched_at":       fetched_at,
        "total_fetched":    total_fetched,
        "total_new":        total_new,
        "total_duplicates": total_fetched - total_new,
        "files_written":    files_written,
        "sources":          results,
    }
