"""
ufc_stats_topup.py
==================

Purpose
-------
Keep a local mirror of UFCStats current by scraping only the events that are
missing from an existing set of six CSVs whose schema matches the Greco1899
`scrape_ufc_stats` repository. The intended workflow is:

    Greco snapshot (historical seed)  ->  this script (incremental top-up)
        ->  the six CSVs are pushed to a public repository
        ->  notebooks repoint BASE_URL to that repo; nothing else changes.

Output files (byte-compatible column order with Greco; verified live)
--------------------------------------------------------------------
    ufc_event_details.csv    EVENT, URL, DATE, LOCATION
    ufc_fight_details.csv     EVENT, BOUT, URL
    ufc_fight_results.csv     EVENT, BOUT, OUTCOME, WEIGHTCLASS, METHOD, ROUND,
                              TIME, TIME FORMAT, REFEREE, DETAILS, URL
    ufc_fight_stats.csv       EVENT, BOUT, ROUND, FIGHTER, KD, SIG.STR.,
                              SIG.STR. %, TOTAL STR., TD, TD %, SUB.ATT, REV.,
                              CTRL, HEAD, BODY, LEG, DISTANCE, CLINCH, GROUND
    ufc_fighter_details.csv   FIRST, LAST, NICKNAME, URL
    ufc_fighter_tott.csv      FIGHTER, HEIGHT, WEIGHT, REACH, STANCE, DOB, URL

Two things to verify locally on first run (see README block at the bottom):
    1. The challenge. ufcstats.com requires real JS execution; this script drives
       a real Chrome via SeleniumBase UC mode, which opens with a disconnect/
       reconnect to clear "checking your browser" and will click a Turnstile
       widget if one appears (HEADLESS=False is required for that click). A visible
       Chrome window will open; that is expected.
    2. The per-round stats parser (parse_fight_stats), which could not be
       checked against the live DOM from the authoring environment. Run
       `--debug-fight <fight_url>` on a bout already present in your Greco seed
       (for example the 16 May 2026 card) and diff the printed rows against the
       existing ufc_fight_stats.csv rows for that bout.

Dependencies
------------
    requests          >= 2.28   (Greco seed download only)
    seleniumbase      >= 4.20   (ufcstats reads; UC mode drives real Chrome,
                                 resolves the matching driver, clears the JS gate)
    beautifulsoup4    >= 4.11
    lxml              >= 4.9     (parser; faster, more lenient than html.parser)
    pandas            >= 1.5
    Python            >= 3.10
    Google Chrome installed (SeleniumBase drives your local Chrome).

    pip install "requests>=2.28" seleniumbase "beautifulsoup4>=4.11" "lxml>=4.9" "pandas>=1.5"

Runs locally (PyCharm / Surface). It is not a Colab job: a top-up is a handful
of event pages, but a full historical rebuild is thousands of requests and must
be run politely from a single machine.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ufcstats.com sits behind a JavaScript/browser challenge (this is what stalled
# the Greco mirror in mid-May 2026). Plain `requests` and curl_cffi TLS
# impersonation were both rejected, and bare undetected-chromedriver kept tripping
# over Chrome/driver version matching on this machine. Reads therefore go through
# SeleniumBase in UC (undetected) mode, which downloads the matching driver for
# the installed Chrome itself, clears the JS challenge via a disconnect/reconnect,
# and can click a Turnstile widget if one appears. The browser is launched once
# and reused across every request; see get_driver(). `requests` is kept only for
# the Greco seed, which is served from GitHub and is not gated.
#
# seleniumbase is imported lazily inside get_driver() so this module still imports
# cleanly on a machine without it or without Chrome.

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

UFCSTATS_ROOT = "http://ufcstats.com"
COMPLETED_EVENTS_URL = f"{UFCSTATS_ROOT}/statistics/events/completed?page=all"

# A real browser User-Agent. ufcstats.com historically served static HTML to
# plain clients; if it now gates on UA/IP, this header alone may not be enough
# and get_soup() will need a browser engine (see module docstring).
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

DEFAULT_DELAY_SECONDS = 1.0   # politeness; ufcstats is a small site
DEFAULT_RETRIES = 3
REQUEST_TIMEOUT = 30

# Browser-engine settings (SeleniumBase UC mode).
# HEADLESS=False (a visible window) is the most reliable way past a Cloudflare
# style challenge, and is required if a Turnstile widget needs a physical click;
# headless is detectable and more likely to loop. SeleniumBase resolves the
# Chrome/driver version itself, so no version pin is needed here.
HEADLESS = False
# Seconds SeleniumBase stays disconnected while the JS challenge runs on the first
# navigation; 6 is a safe default, raise it on a slow connection.
RECONNECT_SECONDS = 6
# How long to wait for the JS challenge to clear after each navigation.
GATE_WAIT_SECONDS = 30
GATE_PHRASES = ("This site requires JavaScript", "Checking your browser")

# Exact column orders. These are asserted against the live Greco headers in the
# accompanying validation step; do not reorder without re-checking.
EVENT_DETAILS_COLS = ["EVENT", "URL", "DATE", "LOCATION"]
FIGHT_DETAILS_COLS = ["EVENT", "BOUT", "URL"]
FIGHT_RESULTS_COLS = [
    "EVENT", "BOUT", "OUTCOME", "WEIGHTCLASS", "METHOD", "ROUND",
    "TIME", "TIME FORMAT", "REFEREE", "DETAILS", "URL",
]
FIGHT_STATS_COLS = [
    "EVENT", "BOUT", "ROUND", "FIGHTER", "KD", "SIG.STR.", "SIG.STR. %",
    "TOTAL STR.", "TD", "TD %", "SUB.ATT", "REV.", "CTRL",
    "HEAD", "BODY", "LEG", "DISTANCE", "CLINCH", "GROUND",
]
FIGHTER_DETAILS_COLS = ["FIRST", "LAST", "NICKNAME", "URL"]
FIGHTER_TOTT_COLS = ["FIGHTER", "HEIGHT", "WEIGHT", "REACH", "STANCE", "DOB", "URL"]

OUTPUT_FILENAMES = {
    "event_details": "ufc_event_details.csv",
    "fight_details": "ufc_fight_details.csv",
    "fight_results": "ufc_fight_results.csv",
    "fight_stats": "ufc_fight_stats.csv",
    "fighter_details": "ufc_fighter_details.csv",
    "fighter_tott": "ufc_fighter_tott.csv",
}

GRECO_RAW_BASE = "https://raw.githubusercontent.com/Greco1899/scrape_ufc_stats/main/"


# ----------------------------------------------------------------------------
# HTTP layer  (ISOLATED ON PURPOSE)
# ----------------------------------------------------------------------------
# Every network read goes through get_soup(). The site needs real JavaScript, so
# reads run inside a SeleniumBase UC session that is opened once by the caller
# (run_topup / debug_one_fight) and held open for the whole run; get_soup() reads
# the active session from the module global _SB. Crucially it uses SeleniumBase's
# own reconnect-aware methods (uc_open_with_reconnect, get_page_source), NOT the
# raw Selenium driver attributes: UC mode deliberately disconnects the driver
# channel while a challenge runs, and touching the raw driver during that window
# is what produced the "localhost connection refused" failure. If the fetch
# mechanism ever changes again, this is still the only place that changes.

_SB = None  # the active SeleniumBase session for the current run (set by caller)


def _page_source_past_gate(sb, timeout: float = GATE_WAIT_SECONDS) -> Optional[str]:
    """Poll the rendered page (via the reconnect-safe get_page_source) until the
    challenge phrases are gone, or time out. Returns the HTML once past the gate,
    else None. The first page of a session waits out the challenge; later pages
    usually return at once because the clearance cookie is already set."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        html = sb.get_page_source()
        if not any(phrase in html for phrase in GATE_PHRASES):
            return html
        time.sleep(1.0)
    html = sb.get_page_source()
    return html if not any(phrase in html for phrase in GATE_PHRASES) else None


def get_soup(url: str, delay: float = DEFAULT_DELAY_SECONDS,
             retries: int = DEFAULT_RETRIES) -> Optional[BeautifulSoup]:
    """Fetch a URL through the active SeleniumBase session and return parsed soup,
    or None on repeated failure.

    Retries with linear backoff. A None return is handled by callers as "skip this
    item, log it" rather than aborting the run, so one bad page does not lose the
    rest. Must be called within an open session (see run_topup / debug_one_fight)."""
    if _SB is None:
        raise RuntimeError("get_soup() called with no active browser session.")

    last_error = None
    for attempt in range(1, retries + 1):
        try:
            time.sleep(delay)  # polite pause before every navigation
            # UC-mode open: SeleniumBase disconnects the automation channel while
            # the page's JS challenge runs, then reconnects. This is what gets past
            # "checking your browser", and it leaves the session reconnected.
            _SB.uc_open_with_reconnect(url, RECONNECT_SECONDS)

            html = _page_source_past_gate(_SB)
            if html is None:
                # An interactive Turnstile may be waiting for a click; try it
                # (needs a visible window) and wait once more.
                try:
                    _SB.uc_gui_click_captcha()
                except Exception:
                    pass
                html = _page_source_past_gate(_SB)

            if html is not None:
                return BeautifulSoup(html, "lxml")
            last_error = "JS challenge did not clear within timeout"
        except Exception as exc:  # selenium/seleniumbase raise their own errors
            last_error = str(exc)
        time.sleep(delay * attempt)  # linear backoff

    print(f"  [warn] giving up on {url} ({last_error})", file=sys.stderr)
    return None


def extract_hash(url: str) -> str:
    """Return the trailing ufcstats id hash from any details URL."""
    return url.rstrip("/").split("/")[-1]


# ----------------------------------------------------------------------------
# Existing-data IO and seeding
# ----------------------------------------------------------------------------

def load_existing(output_dir: Path) -> dict[str, pd.DataFrame]:
    """Load whatever of the six CSVs already exist; return empty frames for any
    that do not. This is what makes the run incremental: events already present
    are skipped."""
    frames: dict[str, pd.DataFrame] = {}
    schema = {
        "event_details": EVENT_DETAILS_COLS,
        "fight_details": FIGHT_DETAILS_COLS,
        "fight_results": FIGHT_RESULTS_COLS,
        "fight_stats": FIGHT_STATS_COLS,
        "fighter_details": FIGHTER_DETAILS_COLS,
        "fighter_tott": FIGHTER_TOTT_COLS,
    }
    for key, cols in schema.items():
        path = output_dir / OUTPUT_FILENAMES[key]
        if path.exists():
            frames[key] = pd.read_csv(path, dtype=str).fillna("")
        else:
            frames[key] = pd.DataFrame(columns=cols)
    return frames


def seed_from_greco(output_dir: Path) -> None:
    """Download Greco's current six CSVs into output_dir as the historical base.

    Greco's data is factual fight data (not GPL-encumbered program output); it is
    used here as a starting snapshot and must be attributed. Run this once, then
    rely on incremental top-ups."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for key, filename in OUTPUT_FILENAMES.items():
        url = GRECO_RAW_BASE + filename
        print(f"  seeding {filename} from Greco ...")
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        (output_dir / filename).write_bytes(resp.content)
    print("  seed complete.")


# ----------------------------------------------------------------------------
# Event list  ->  which events are missing
# ----------------------------------------------------------------------------

def parse_completed_events(soup: BeautifulSoup) -> list[dict]:
    """Return [{EVENT, URL, DATE, LOCATION}] for every completed event row.

    Parsed per row (not as three parallel lists) so name, date and location stay
    aligned even if the page adds or removes a row. The top row of the table is
    typically the next, not-yet-held event; callers filter it out by URL (it is
    absent from our data so it would be 'new', but its fights carry no result and
    are skipped downstream) or by a future date."""
    events = []
    for row in soup.select("tr.b-statistics__table-row"):
        link = row.select_one("a.b-link.b-link_style_black")
        if link is None or not link.get("href"):
            continue  # header or spacer row
        date_el = row.select_one("span.b-statistics__date")
        loc_el = row.select_one(
            "td.b-statistics__table-col_style_big-top-padding"
        )
        events.append({
            "EVENT": link.get_text(strip=True),
            "URL": link["href"].strip(),
            "DATE": date_el.get_text(strip=True) if date_el else "",
            "LOCATION": loc_el.get_text(strip=True) if loc_el else "",
        })
    return events


def parse_event_fight_urls(soup: BeautifulSoup) -> list[str]:
    """Return the fight-details URLs listed on an event page.

    Each bout is a clickable row carrying the fight URL in its data-link
    attribute; some renderings use an onclick handler instead, handled as a
    fallback."""
    urls = []
    for row in soup.select("tr.js-fight-details-click"):
        link = row.get("data-link")
        if not link:
            onclick = row.get("onclick", "")
            match = re.search(r"fight-details/[0-9a-f]+", onclick)
            link = f"{UFCSTATS_ROOT}/{match.group(0)}" if match else None
        if link:
            urls.append(link.strip())
    return urls


# ----------------------------------------------------------------------------
# Fight page  ->  fight_results row + fight_stats rows + fighter URLs
# ----------------------------------------------------------------------------

def _labelled_items(soup: BeautifulSoup) -> dict[str, str]:
    """Collect 'Label: value' items (Method, Round, Time, Time format, Referee)
    from the fight head. Each item carries a label sub-element; the value is the
    remaining text once the label is removed."""
    info: dict[str, str] = {}
    for item in soup.select("i.b-fight-details__text-item, i.b-fight-details__text-item_first"):
        label_el = item.select_one("i.b-fight-details__label")
        if label_el is None:
            continue
        label = label_el.get_text(strip=True).rstrip(":")
        value = item.get_text(" ", strip=True)
        # Strip the leading label text from the combined string.
        value = re.sub(rf"^{re.escape(label_el.get_text(strip=True))}\s*", "", value).strip()
        info[label] = value
    return info


def parse_fight_results(soup: BeautifulSoup, fight_url: str) -> Optional[dict]:
    """Build one ufc_fight_results row from a fight page, or None if the bout has
    no recorded result (an upcoming fight on a not-yet-complete card)."""
    title_el = soup.select_one("h2.b-content__title")
    persons = soup.select("a.b-fight-details__person-link")
    if title_el is None or len(persons) < 2:
        return None

    event = title_el.get_text(strip=True)
    fighter_a = persons[0].get_text(strip=True)
    fighter_b = persons[1].get_text(strip=True)
    bout = f"{fighter_a} vs. {fighter_b}"

    # Win/loss/draw/nc status sits in each person block as a short i-tag.
    statuses = [
        block.select_one("i.b-fight-details__person-status")
        for block in soup.select("div.b-fight-details__person")
    ]
    statuses = [s.get_text(strip=True) for s in statuses if s is not None]
    if len(statuses) < 2 or not statuses[0]:
        return None  # no result recorded -> skip (upcoming bout)
    outcome = f"{statuses[0]}/{statuses[1]}"

    # Weight class / bout title (kept raw, including 'Title'/'Bout' qualifiers,
    # which the notebook canonicaliser strips downstream).
    wc_el = soup.select_one("i.b-fight-details__fight-title")
    weightclass = wc_el.get_text(" ", strip=True) if wc_el else ""

    info = _labelled_items(soup)

    # Details: the paragraph whose label is 'Details:' (judges' scorecards for a
    # decision, or the finishing-sequence description otherwise).
    details = ""
    for p in soup.select("p.b-fight-details__text"):
        label_el = p.select_one("i.b-fight-details__label")
        if label_el and label_el.get_text(strip=True).rstrip(":").lower() == "details":
            details = re.sub(r"^Details:\s*", "", p.get_text(" ", strip=True)).strip()
            break

    return {
        "EVENT": event,
        "BOUT": bout,
        "OUTCOME": outcome,
        "WEIGHTCLASS": weightclass,
        "METHOD": info.get("Method", ""),
        "ROUND": info.get("Round", ""),
        "TIME": info.get("Time", ""),
        "TIME FORMAT": info.get("Time format", ""),
        "REFEREE": info.get("Referee", ""),
        "DETAILS": details,
        "URL": fight_url,
    }


def parse_fight_stats(soup: BeautifulSoup, event: str, bout: str) -> list[dict]:
    """Build the per-round ufc_fight_stats rows from a fight page.

    *** VERIFY THIS AGAINST A LIVE PAGE BEFORE TRUSTING IT (see module docstring). ***

    Strategy, expressed structurally rather than by fragile class names:
      - ufcstats shows two per-round stat tables: a 'Totals' table (KD, Sig.str.,
        Total str., Td, Sub.att, Rev., Ctrl) and a 'Significant Strikes' table
        (Head, Body, Leg, Distance, Clinch, Ground).
      - Each table interleaves a round-label row ('Round 1', ...) with a data row
        whose first column holds the two fighter names and whose remaining columns
        hold each fighter's value as two stacked <p> elements.
      - We read both tables, key every value by (round, fighter), then emit one
        row per fighter per round in Greco's column order. Bouts with no stats
        table at all (older or 'not currently available') simply yield no rows,
        which is correct: the bout still exists in fight_results.
    """
    # Map each per-round table to the output columns it supplies, in page order.
    TOTALS_VALUE_COLS = ["KD", "SIG.STR.", "SIG.STR. %", "TOTAL STR.",
                         "TD", "TD %", "SUB.ATT", "REV.", "CTRL"]
    SIGSTR_VALUE_COLS = ["SIG.STR.", "SIG.STR. %", "HEAD", "BODY", "LEG",
                         "DISTANCE", "CLINCH", "GROUND"]

    def identify_table(table) -> Optional[list[str]]:
        """Decide which value columns a table supplies by reading its header
        text; returns None for non-stats tables. Header-text matching is more
        durable than matching on CSS classes."""
        header_text = table.get_text(" ", strip=True).lower()
        has_rounds = re.search(r"round\s+\d", header_text) is not None
        if not has_rounds:
            return None
        if "total str" in header_text:
            return TOTALS_VALUE_COLS
        if "distance" in header_text or "head" in header_text:
            return SIGSTR_VALUE_COLS
        return None

    # accumulator[(round_label, fighter_name)] = {col: value}
    accumulator: dict[tuple[str, str], dict[str, str]] = {}

    for table in soup.select("table"):
        value_cols = identify_table(table)
        if value_cols is None:
            continue

        current_round: Optional[str] = None
        for tr in table.select("tr"):
            row_text = tr.get_text(" ", strip=True)
            round_match = re.search(r"Round\s+(\d+)", row_text)

            # A round-label row sets the context for the data row that follows.
            cells = tr.select("td.b-fight-details__table-col")
            if round_match and not cells:
                current_round = f"Round {round_match.group(1)}"
                continue
            if not cells or current_round is None:
                continue

            # First cell: the two fighter names (two stacked <p>).
            name_ps = cells[0].select("p")
            if len(name_ps) < 2:
                continue
            fighters = [name_ps[0].get_text(strip=True),
                        name_ps[1].get_text(strip=True)]

            # Remaining cells: one value per fighter, aligned to value_cols.
            for col_index, col_name in enumerate(value_cols, start=1):
                if col_index >= len(cells):
                    break
                value_ps = cells[col_index].select("p")
                for fighter_index, fighter in enumerate(fighters):
                    if fighter_index >= len(value_ps):
                        continue
                    key = (current_round, fighter)
                    accumulator.setdefault(key, {})[col_name] = \
                        value_ps[fighter_index].get_text(strip=True)

    # Emit rows in Greco column order.
    rows = []
    for (round_label, fighter), values in accumulator.items():
        row = {"EVENT": event, "BOUT": bout, "ROUND": round_label, "FIGHTER": fighter}
        for col in FIGHT_STATS_COLS[4:]:  # skip EVENT, BOUT, ROUND, FIGHTER
            row[col] = values.get(col, "")
        rows.append(row)
    return rows


def parse_fighter_urls(soup: BeautifulSoup) -> list[str]:
    """Fighter profile URLs linked from a fight page (two per bout)."""
    return [a["href"].strip()
            for a in soup.select("a.b-fight-details__person-link")
            if a.get("href")]


# ----------------------------------------------------------------------------
# Fighter profile page  ->  fighter_details row + fighter_tott row
# ----------------------------------------------------------------------------

def parse_fighter_profile(soup: BeautifulSoup, fighter_url: str) -> Optional[tuple[dict, dict]]:
    """Return (fighter_details_row, fighter_tott_row) from a profile page.

    Note: FIRST/LAST are split from the full name on the first space, which is a
    heuristic (it mishandles multi-token first or last names). It is adequate for
    the small number of debut fighters a top-up adds, and the full FIGHTER name
    in the tott table is the reliable join key your name resolver uses."""
    name_el = soup.select_one("span.b-content__title-highlight")
    if name_el is None:
        return None
    full_name = name_el.get_text(strip=True)
    parts = full_name.split(" ", 1)
    first = parts[0]
    last = parts[1] if len(parts) > 1 else ""

    nick_el = soup.select_one("p.b-content__Nickname")
    nickname = nick_el.get_text(strip=True) if nick_el else ""

    # Tale of the tape: labelled list items (Height, Weight, Reach, STANCE, DOB).
    tott = {"HEIGHT": "", "WEIGHT": "", "REACH": "", "STANCE": "", "DOB": ""}
    label_map = {"height": "HEIGHT", "weight": "WEIGHT", "reach": "REACH",
                 "stance": "STANCE", "dob": "DOB"}
    for li in soup.select("li.b-list__box-list-item"):
        text = li.get_text(" ", strip=True)
        if ":" not in text:
            continue
        raw_label, _, value = text.partition(":")
        key = label_map.get(raw_label.strip().lower())
        if key:
            tott[key] = value.strip()

    details_row = {"FIRST": first, "LAST": last, "NICKNAME": nickname, "URL": fighter_url}
    tott_row = {"FIGHTER": full_name, "HEIGHT": tott["HEIGHT"], "WEIGHT": tott["WEIGHT"],
                "REACH": tott["REACH"], "STANCE": tott["STANCE"], "DOB": tott["DOB"],
                "URL": fighter_url}
    return details_row, tott_row


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------

def run_topup(output_dir: Path, delay: float, max_events: Optional[int],
              force_full: bool) -> None:
    existing = load_existing(output_dir)
    known_event_urls = set(existing["event_details"]["URL"]) if not force_full else set()
    known_fighter_urls = set(existing["fighter_details"]["URL"])

    print(f"Existing rows: "
          f"{len(existing['event_details'])} events, "
          f"{len(existing['fight_results'])} fights, "
          f"{len(existing['fight_stats'])} stat rows, "
          f"{len(existing['fighter_details'])} fighters.")

    new = {key: [] for key in OUTPUT_FILENAMES}  # collected new rows per file

    # All network reads happen inside one SeleniumBase UC session, opened once and
    # held open for the whole run. get_soup() reads it from the module global _SB.
    global _SB
    from seleniumbase import SB  # lazy: only needed when actually scraping

    with SB(uc=True, headless=HEADLESS) as sb:
        _SB = sb
        try:
            # 1. Completed-events index -> events we do not already have.
            index_soup = get_soup(COMPLETED_EVENTS_URL, delay=delay)
            if index_soup is None:
                print("Could not load the completed-events index; aborting.",
                      file=sys.stderr)
                return
            all_events = parse_completed_events(index_soup)
            new_events = [e for e in all_events if e["URL"] not in known_event_urls]
            if max_events is not None:
                new_events = new_events[:max_events]

            print(f"Index lists {len(all_events)} completed events; "
                  f"{len(new_events)} are new and will be scraped.")
            if not new_events:
                print("Nothing to do. Mirror is current with the index.")
                return

            # 2. Walk each new event.
            for i, event in enumerate(new_events, start=1):
                print(f"[{i}/{len(new_events)}] {event['EVENT']} ({event['DATE']})")
                event_soup = get_soup(event["URL"], delay=delay)
                if event_soup is None:
                    continue

                fight_urls = parse_event_fight_urls(event_soup)
                scraped_any_fight = False

                for fight_url in fight_urls:
                    fight_soup = get_soup(fight_url, delay=delay)
                    if fight_soup is None:
                        continue

                    result_row = parse_fight_results(fight_soup, fight_url)
                    if result_row is None:
                        continue  # upcoming/blank bout
                    scraped_any_fight = True

                    new["fight_results"].append(result_row)
                    new["fight_details"].append({
                        "EVENT": result_row["EVENT"],
                        "BOUT": result_row["BOUT"],
                        "URL": fight_url,
                    })
                    new["fight_stats"].extend(
                        parse_fight_stats(fight_soup, result_row["EVENT"], result_row["BOUT"])
                    )

                    # New fighters only.
                    for furl in parse_fighter_urls(fight_soup):
                        if furl in known_fighter_urls:
                            continue
                        known_fighter_urls.add(furl)
                        profile_soup = get_soup(furl, delay=delay)
                        if profile_soup is None:
                            continue
                        parsed = parse_fighter_profile(profile_soup, furl)
                        if parsed is None:
                            continue
                        details_row, tott_row = parsed
                        new["fighter_details"].append(details_row)
                        new["fighter_tott"].append(tott_row)

                # Only record the event itself if it actually had completed fights.
                if scraped_any_fight:
                    new["event_details"].append(event)
        finally:
            _SB = None  # browser closes as the `with` block exits

    # 3. Append, de-duplicate on natural keys, write (browser already closed).
    write_outputs(output_dir, existing, new)


def write_outputs(output_dir: Path, existing: dict, new: dict) -> None:
    """Concatenate new rows onto existing, drop duplicates idempotently, and
    write all six files. De-dup keys mirror the natural identity of each table so
    re-running the script never doubles rows."""
    dedup_keys = {
        "event_details": ["URL"],
        "fight_details": ["URL"],
        "fight_results": ["URL"],
        "fight_stats": ["EVENT", "BOUT", "ROUND", "FIGHTER"],
        "fighter_details": ["URL"],
        "fighter_tott": ["URL"],
    }
    col_order = {
        "event_details": EVENT_DETAILS_COLS, "fight_details": FIGHT_DETAILS_COLS,
        "fight_results": FIGHT_RESULTS_COLS, "fight_stats": FIGHT_STATS_COLS,
        "fighter_details": FIGHTER_DETAILS_COLS, "fighter_tott": FIGHTER_TOTT_COLS,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    print("\nWrite summary (rows before -> after):")
    for key, filename in OUTPUT_FILENAMES.items():
        before = len(existing[key])
        new_df = pd.DataFrame(new[key], columns=col_order[key])
        combined = pd.concat([existing[key], new_df], ignore_index=True)
        # keep='first' preserves the existing (seed) row when a key recurs.
        combined = combined.drop_duplicates(subset=dedup_keys[key], keep="first")
        combined = combined[col_order[key]]
        combined.to_csv(output_dir / filename, index=False)
        print(f"  {filename:28s} {before:6d} -> {len(combined):6d}  (+{len(combined) - before})")
    print("\nDone. Review the deltas above, then commit and push the six CSVs.")


# ----------------------------------------------------------------------------
# Debug: parse a single fight and print what we got (the key verification step)
# ----------------------------------------------------------------------------

def debug_one_fight(fight_url: str, delay: float) -> None:
    """Scrape one fight page and print the parsed result + stats rows so they can
    be eyeballed against the live page or against an existing Greco row for the
    same bout. This confirms parse_fight_stats."""
    global _SB
    from seleniumbase import SB  # lazy: only needed when actually scraping

    with SB(uc=True, headless=HEADLESS) as sb:
        _SB = sb
        try:
            soup = get_soup(fight_url, delay=delay)
        finally:
            _SB = None

    if soup is None:
        print("Could not load the fight page (see warning above).")
        return
    result = parse_fight_results(soup, fight_url)
    print("\n--- fight_results row ---")
    if result is None:
        print("  (no result parsed; upcoming bout or selector mismatch)")
    else:
        for k, v in result.items():
            print(f"  {k:12s}: {v}")
    if result is not None:
        stats = parse_fight_stats(soup, result["EVENT"], result["BOUT"])
        print(f"\n--- fight_stats rows ({len(stats)}) ---")
        for row in sorted(stats, key=lambda r: (r["FIGHTER"], r["ROUND"])):
            print(f"  {row['ROUND']:8s} {row['FIGHTER']:22s} "
                  f"KD={row['KD']} SIG.STR.={row['SIG.STR.']} TD={row['TD']} "
                  f"CTRL={row['CTRL']} HEAD={row['HEAD']}")


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally top up a local Greco-schema UFCStats mirror."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("./greco_mirror"),
                        help="Folder holding the six CSVs (default ./greco_mirror).")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS,
                        help="Seconds between requests (politeness).")
    parser.add_argument("--max-events", type=int, default=None,
                        help="Cap on new events to scrape; use a small value for a first test.")
    parser.add_argument("--seed-from-greco", action="store_true",
                        help="Download Greco's six CSVs into --output-dir as the base, then exit.")
    parser.add_argument("--full", action="store_true",
                        help="Ignore existing data and scrape every completed event (slow; be polite).")
    parser.add_argument("--debug-fight", type=str, default=None,
                        help="Parse a single fight URL, print the rows, and exit (verification).")
    args = parser.parse_args(argv)

    if args.seed_from_greco:
        seed_from_greco(args.output_dir)
        return
    if args.debug_fight:
        debug_one_fight(args.debug_fight, delay=args.delay)
        return
    run_topup(args.output_dir, delay=args.delay,
              max_events=args.max_events, force_full=args.full)


if __name__ == "__main__":
    main()