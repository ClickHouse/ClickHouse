"""Nightly job: label GitHub issues opened by external contributors.

Attaches the `external` label to issues whose author is not a member of the
ClickHouse GitHub organization. By default it scans issues created in the last
`--days` days (3, matching the nightly cadence with margin); pass `--all` to
backfill the entire issue history in one run.

The companion pre-hook `can_be_tested`
(`ci/jobs/scripts/workflow_hooks/trusted.py`) labels pull requests from external
contributors the same way at pull-request time, so the two together keep both
issues and pull requests marked. Membership and the label constant/helpers are
shared with that hook.

Only issues are labeled here: the repository issues endpoint returns pull
requests too, and those are filtered out by their `pull_request` field.
"""

import argparse
import datetime
import json
import shlex
import sys

from ci.jobs.scripts.workflow_hooks.trusted import (
    EXTERNAL_LABEL,
    INTERNAL_BOTS,
    get_org_members,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

DEFAULT_DAYS = 3


def _parse_iso8601(value: str) -> datetime.datetime:
    """Parse a GitHub timestamp ('2026-09-10T12:00:00Z') as an aware UTC time."""
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_issues(repo: str, since: datetime.datetime = None) -> list:
    """Every issue of the repository (pull requests excluded), newest activity
    paginated. `since` bounds the fetch to issues updated at or after that time;
    an issue created within a window was necessarily updated within it too, so
    this is a safe superset for the created-within filter the caller applies."""
    url = f"repos/{repo}/issues?state=all&per_page=100"
    if since is not None:
        url += f"&since={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    # --slurp collapses the paginated pages into one JSON array of page arrays.
    out = Shell.get_output(
        f"gh api --paginate --slurp {shlex.quote(url)}",
        verbose=True,
        strict=True,
    )
    issues = []
    for page in json.loads(out):
        for item in page:
            if "pull_request" in item:  # the endpoint returns PRs too - skip them
                continue
            issues.append(item)
    return issues


def label_external_issues(days: int, backfill: bool) -> bool:
    repo = Info().repo_name
    members = get_org_members()
    print(f"ClickHouse organization has {len(members)} members")

    if backfill:
        since = None
        cutoff = None
        print("Backfill mode: scanning the entire issue history")
    else:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=days
        )
        since = cutoff
        print(f"Scanning issues created since {cutoff.isoformat()}")

    issues = fetch_issues(repo, since=since)
    print(f"Fetched {len(issues)} issue(s)")

    labeled = []
    for issue in issues:
        number = issue["number"]
        author = (issue.get("user") or {}).get("login", "")
        created = _parse_iso8601(issue["created_at"])
        if cutoff is not None and created < cutoff:
            continue
        if any(label["name"] == EXTERNAL_LABEL for label in issue.get("labels", [])):
            continue
        if author.lower() in members or author.lower() in INTERNAL_BOTS:
            continue
        print(f"Labeling #{number} by external author '{author}' as '{EXTERNAL_LABEL}'")
        if not Shell.check(
            f"gh issue edit {number} --repo {shlex.quote(repo)} "
            f"--add-label {EXTERNAL_LABEL}",
            verbose=True,
        ):
            print(f"ERROR: failed to label #{number}", file=sys.stderr)
            return False
        labeled.append(number)

    print(f"Labeled {len(labeled)} issue(s) as '{EXTERNAL_LABEL}': {labeled}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"scan issues created within this many days (default {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="backfill",
        help="backfill: scan the entire issue history regardless of creation date",
    )
    args = parser.parse_args()

    Result.from_commands_run(
        name="Label external issues",
        command=lambda: label_external_issues(args.days, args.backfill),
    ).complete_job()
