#!/usr/bin/env python3
"""
Weekly Job Search Bot (GitHub Actions-friendly)

Sources (no-proxy-friendly):
- Remotive (public API)
- We Work Remotely (RSS)

Writes top ~N filtered jobs into a Google Sheet tab "Weekly_Role_Search"
using a Google service account (from JSON in env/secret).

ENV VARS (set in GitHub Actions secrets or workflow env):
- GOOGLE_SERVICE_ACCOUNT_JSON : full JSON string of the service account key
- GOOGLE_SHEET_ID             : your Google Sheet ID
- WORKSHEET_NAME              : defaults to 'Weekly_Role_Search'
- KEYWORDS                    : comma-separated (e.g. "power bi, sql, sap fi")
- MAX_TOTAL                   : integer (e.g. "20")
"""
import os, json, logging, datetime, re, html
from typing import List, Dict, Any
import requests, gspread, feedparser
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

def monday_of_week(d: datetime.date) -> datetime.date:
    return d - datetime.timedelta(days=d.weekday())

def get_ws(sheet_id: str, worksheet_name: str, service_json_path: str):
    creds = ServiceAccountCredentials.from_json_keyfile_name(service_json_path, SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(worksheet_name)

def matches_keywords(text: str, keywords: List[str]) -> List[str]:
    t = text.lower()
    return [kw for kw in keywords if kw and kw.lower().strip() in t]

# -------- Sources --------
def fetch_remotive() -> List[Dict[str, Any]]:
    url = "https://remotive.com/api/remote-jobs"
    r = requests.get(url, timeout=40); r.raise_for_status()
    data = r.json(); jobs = []
    for item in data.get("jobs", []):
        title = item.get("title",""); company = item.get("company_name","")
        location = item.get("candidate_required_location","Worldwide")
        link = item.get("url",""); posted = item.get("publication_date","")[:10]
        desc = " ".join(item.get("tags") or [])
        jobs.append({"source":"Remotive","title":title,"company":company,"location":location,
                     "remote":"Worldwide","posted":posted,"link":link,"notes":desc})
    logging.info("Fetched %d from Remotive", len(jobs))
    return jobs

def fetch_wwr() -> List[Dict[str, Any]]:
    urls = [
        "https://weworkremotely.com/categories/remote-programming-jobs.rss",
        "https://weworkremotely.com/categories/remote-data-jobs.rss",
        "https://weworkremotely.com/categories/remote-sales-and-marketing-jobs.rss",
    ]
    jobs = []
    for u in urls:
        d = feedparser.parse(u)
        for e in d.entries:
            title = html.unescape(e.get("title",""))
            link  = e.get("link","")
            summary = html.unescape(e.get("summary",""))
            m = re.search(r"<(?:strong|b)>(.*?)</", summary, re.I)
            company = m.group(1) if m else ""
            posted = e.get("published","")[:16]
            jobs.append({"source":"WeWorkRemotely","title":title,"company":company,"location":"Worldwide",
                         "remote":"Worldwide","posted":posted,"link":link,"notes":summary})
    logging.info("Fetched %d from WWR", len(jobs))
    return jobs

# -------- Main --------
def main():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID","").strip()
    worksheet_name = os.environ.get("WORKSHEET_NAME","Weekly_Role_Search").strip()
    max_total = int(os.environ.get("MAX_TOTAL","20"))
    keywords_raw = os.environ.get("KEYWORDS","business intelligence, bi analyst, financial data analyst, power bi, sql, sap fi, finance transformation, rpa")
    keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
    if not sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID not provided")

    service_json_path = "service_account.json"
    if not os.path.exists(service_json_path):
        sa_json_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
        if not sa_json_env:
            raise RuntimeError("service_account.json missing and GOOGLE_SERVICE_ACCOUNT_JSON not set")
        with open(service_json_path, "w", encoding="utf-8") as f:
            f.write(sa_json_env)

    all_jobs: List[Dict[str, Any]] = []
    for fn in (fetch_remotive, fetch_wwr):
        try:
            all_jobs.extend(fn())
        except Exception as e:
            logging.warning("Source failed: %s", e)

    filtered: List[Dict[str, Any]] = []
    for j in all_jobs:
        text = " ".join([j.get("title",""), j.get("company",""), j.get("notes","")])
        hits = matches_keywords(text, keywords)
        if hits:
            j["hits"] = ", ".join(hits)
            filtered.append(j)

    filtered.sort(key=lambda x: (x.get("posted",""), x.get("title","")), reverse=True)
    filtered = filtered[:max_total]

    ws = get_ws(sheet_id, worksheet_name, service_json_path)
    ws.clear()
    headers = ["Week Of (Mon)","Source","Job Title","Company","Location","Remote Policy","Posted Date","Link","Notes","Matched Keywords"]
    ws.append_row(headers, value_input_option="RAW")

    week = monday_of_week(datetime.date.today()).isoformat()
    for j in filtered:
        ws.append_row([week, j.get("source",""), j.get("title",""), j.get("company",""),
                       j.get("location",""), j.get("remote","Worldwide"), j.get("posted",""),
                       j.get("link",""), j.get("notes",""), j.get("hits","")], value_input_option="RAW")
    logging.info("Wrote %d rows to %s!%s", len(filtered), sheet_id, worksheet_name)

if __name__ == "__main__":
    main()
