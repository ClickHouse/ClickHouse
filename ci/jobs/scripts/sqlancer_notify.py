#!/usr/bin/env python3
"""Post a Slack alert when a SQLancer run surfaces a failure we have not seen before.

A nightly fuzzer that alerts on every failure trains everyone to ignore it: the
same known bug is re-found every run until it is fixed upstream. So this alerts
on NEW fingerprints only.

"New" is answered from CI DB itself rather than from a state file: praktika
inserts one row per report row (`CIDB.json_data_generator`), and the rows for a
SQLancer job are named after the failure fingerprint, so
`SELECT DISTINCT test_name ... WHERE check_name = <this job>` over the last N days
is exactly the set of failures this job has already reported. The current run's
own rows are inserted after the job script finishes, so they cannot mask a new
finding.

Everything here is best-effort: no webhook, no network, a bad response - the
script says so and exits 0. A notification problem must never change a fuzzing
run's verdict.
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

# The job runs with the sqlancer checkout as its working directory, so the repo
# root has to come from this file's location, not from `.`. Praktika then reads
# its own context from RELATIVE `./ci/tmp` paths (`Settings.TEMP_DIR`,
# `_Environment.get`), so the process also has to sit in the repo root - from
# anywhere else `Info()` silently degrades and the run looks like the scheduled
# master stream with no report links.
ORIGINAL_CWD = Path.cwd()
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

SLACK_WEBHOOK_ENV = "SLACK_WEBHOOK_CORE_QA"
HISTORY_DAYS = 30
MAX_LISTED = 5
TIMEOUT_SEC = 20

# Defaults matching ci/settings/settings.py, used if praktika cannot be imported:
# an alert is more valuable than a perfectly linked one.
CI_DB_READ_URL = "https://play.clickhouse.com"
CI_DB_READ_USER = "play"
CI_DB_NAME = "default"
CI_DB_TABLE = "checks"

try:
    from ci.praktika.info import Info
    from ci.praktika.settings import Settings

    CI_DB_READ_URL = Settings.CI_DB_READ_URL or CI_DB_READ_URL
    CI_DB_READ_USER = Settings.CI_DB_READ_USER or CI_DB_READ_USER
    CI_DB_NAME = Settings.CI_DB_DB_NAME or CI_DB_NAME
    CI_DB_TABLE = Settings.CI_DB_TABLE_NAME or CI_DB_TABLE
except Exception as e:  # noqa: BLE001
    print(f"WARNING: praktika not importable ({e}); alerting without report links")
    Info = None


def http_post(url, data, headers=None, timeout=TIMEOUT_SEC):
    request = urllib.request.Request(url, data=data, headers=headers or {}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status, response.read().decode("utf-8", "replace")


def current_stream():
    """(pull_request_number, head_ref) of this run, as CI DB records them."""
    if Info is not None:
        try:
            info = Info()
            return int(info.pr_number or 0), (info.git_branch or "master")
        except Exception as e:  # noqa: BLE001
            print(f"WARNING: could not read the current branch ({e}); assuming the scheduled master stream")
    return 0, "master"


def known_fingerprints(job_name):
    """Fingerprints this job has already reported, from CI DB (read-only endpoint).

    Scoped to this run's own stream. The workflow is `workflow_dispatch`-able on
    any ref, so a throwaway validation run would otherwise share the seen-set with
    the scheduled master nightlies and suppress the first real master alert for a
    failure it happened to reproduce first.
    """
    pr_number, head_ref = current_stream()
    query = (
        f"SELECT DISTINCT test_name FROM {CI_DB_NAME}.{CI_DB_TABLE} "
        f"WHERE check_name = {sql_string(job_name)} "
        f"AND pull_request_number = {pr_number} "
        f"AND head_ref = {sql_string(head_ref)} "
        f"AND check_start_time > now() - INTERVAL {HISTORY_DAYS} DAY "
        f"AND test_name != ''"
    )
    url = f"{CI_DB_READ_URL}/?" + urllib.parse.urlencode(
        {"user": CI_DB_READ_USER, "default_format": "TSVRaw"}
    )
    status, body = http_post(url, query.encode("utf-8"))
    if status != 200:
        raise RuntimeError(f"CI DB query failed with status {status}: {body[:200]}")
    return {line for line in body.splitlines() if line}


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def slack_blocks(job_name, new_failures, findings, info, extra_failures=0):
    listed = new_failures[:MAX_LISTED]
    lines = []
    for failure in listed:
        shape = (failure.get("message_shapes") or [""])[0]
        lines.append(f"• *{failure['fingerprint']}* (x{failure['count']})\n    `{shape[:180]}`")
    if len(new_failures) > len(listed):
        lines.append(f"• … and {len(new_failures) - len(listed)} more new failure(s)")

    elements = []
    report_url = job_url = ""
    if Info is not None:
        try:
            report_url = Info().get_job_report_url()
            job_url = Info().get_job_url()
        except Exception as e:  # noqa: BLE001
            print(f"WARNING: could not build report links ({e})")
    if report_url:
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "📊 Report", "emoji": True},
                "url": report_url,
            }
        )
    if job_url:
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "📋 Job logs", "emoji": True},
                "url": job_url,
            }
        )

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"🐞 *{job_name}: {len(new_failures)} new failure(s)*\n"
                    + "\n".join(lines)
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{findings.get('findings', 0) + extra_failures} finding(s) in "
                        f"{findings.get('distinct_failures', 0) + extra_failures} distinct failure(s) "
                        f"this run  ·  {info}"
                    ),
                }
            ],
        },
    ]
    if elements:
        blocks.append({"type": "actions", "elements": elements})
    return blocks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--findings", required=True, help="findings.json from sqlancer_failures.py")
    parser.add_argument("--job-name", required=True, help="job name, i.e. the CI DB check_name")
    parser.add_argument("--info", default="", help="one-line run summary for the message footer")
    parser.add_argument(
        "--extra-failure",
        default="",
        help="fingerprint of a failure found outside the oracles (a sanitizer report or a "
        "<Fatal> server message); must be the report row name so it matches CI DB history",
    )
    parser.add_argument("--extra-failure-message", default="", help="first line of that failure")
    parser.add_argument("--dry-run", action="store_true", help="print the message instead of posting it")
    args = parser.parse_args()

    # Resolved against the caller's directory, since this process chdir'd away.
    findings_path = ORIGINAL_CWD / args.findings
    findings = json.loads(findings_path.read_text(encoding="utf-8"))
    failures = findings.get("failures") or []
    if args.extra_failure:
        # A sanitizer report or a `<Fatal>` message is a finding with no oracle
        # reproducer behind it, so it never reaches findings.json - and it is
        # exactly what the sanitizer build exists to catch. Feed it in so it takes
        # part in the new-vs-known diff like any other failure.
        failures = failures + [
            {
                "fingerprint": args.extra_failure,
                "count": 1,
                "message_shapes": [args.extra_failure_message] if args.extra_failure_message else [],
            }
        ]
    if not failures:
        print("No findings - nothing to notify about")
        return

    try:
        seen = known_fingerprints(args.job_name)
        pr_number, head_ref = current_stream()
        print(
            f"CI DB knows {len(seen)} fingerprint(s) for [{args.job_name}] on "
            f"[{head_ref}, PR {pr_number}] from the last {HISTORY_DAYS} days"
        )
    except Exception as e:  # noqa: BLE001 - never fail the job over a query
        # Fail closed. Without the history every known failure looks new, and one
        # play.clickhouse.com blip would post the whole backlog to the channel -
        # which is how an alert channel gets muted for good. The findings are in
        # the job report either way.
        print(f"WARNING: could not read failure history from CI DB ({e}) - skipping the alert")
        return

    new_failures = [f for f in failures if f["fingerprint"] not in seen]
    for failure in failures:
        state = "NEW" if failure["fingerprint"] not in seen else "known"
        print(f"  [{state}] x{failure['count']} {failure['fingerprint']}")
    if not new_failures:
        print("All failures are already known - no alert")
        return

    # The server-log finding has no oracle reproducer, so findings.json does not
    # count it - add it to what the message displays, not just to the diff set.
    blocks = slack_blocks(
        args.job_name, new_failures, findings, args.info, extra_failures=1 if args.extra_failure else 0
    )
    webhook = os.getenv(SLACK_WEBHOOK_ENV)
    if args.dry_run or not webhook:
        if not webhook:
            print(f"{SLACK_WEBHOOK_ENV} not set - would have alerted about {len(new_failures)} new failure(s)")
        print(json.dumps(blocks, indent=1))
        return

    try:
        status, body = http_post(
            webhook,
            json.dumps({"blocks": blocks}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        if status == 200:
            print(f"Alerted about {len(new_failures)} new failure(s)")
        else:
            print(f"WARNING: Slack post failed: {status} {body[:200]}")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: Slack post failed: {e}")


if __name__ == "__main__":
    main()
