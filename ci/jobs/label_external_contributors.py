"""Nightly job: label issues and pull requests from external contributors.

Attaches the `external` label to issues and pull requests whose author is not a
member of the ClickHouse GitHub organization, so external contributions are
easy to filter and triage. By default it scans items created in the last
`--days` days (3, matching the nightly cadence with margin); pass `--all` to
backfill the entire history in one run.

Everything the feature needs lives in this one script: the organization
membership lookup, the bot allowlist, and the labeling of both issues and pull
requests. The repository issues endpoint returns pull requests too (each one
carries a `pull_request` field), so a single scan covers both.

Automation accounts operated by ClickHouse are organization non-members but must
not be treated as external contributors, so they are excluded via INTERNAL_BOTS.
"""

import argparse
import datetime
import json
import shlex
import sys

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

DEFAULT_DAYS = 3

# Marks an issue or pull request whose author is not a member of the ClickHouse
# GitHub organization.
EXTERNAL_LABEL = "external"

# Automation accounts operated by ClickHouse that are not members of the GitHub
# organization but must not be labeled `external`.
INTERNAL_BOTS = {"groeneai", "oranjeai", "clickgapai"}


def fetch_org_members() -> set:
    """The logins (lowercased) of the ClickHouse organization members. Cached by
    `gh` for an hour, so the single call here is cheap even on reruns."""
    lines = Shell.get_output(
        "gh api orgs/ClickHouse/members --paginate --cache=1h --jq='.[].login'",
        verbose=True,
        strict=True,
    )
    return {line.strip().lower() for line in lines.splitlines() if line.strip()}


def _parse_iso8601(value: str) -> datetime.datetime:
    """Parse a GitHub timestamp ('2026-09-10T12:00:00Z') as an aware UTC time."""
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_issues_and_prs(repo: str, since: datetime.datetime = None) -> list:
    """Every issue and pull request of the repository, newest activity
    paginated. `since` bounds the fetch to items updated at or after that time;
    an item created within a window was necessarily updated within it too, so
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
    return [item for page in json.loads(out) for item in page]


def add_external_label(repo: str, number: int, is_pr: bool) -> bool:
    """Add the `external` label to an issue or pull request. Idempotent."""
    kind = "pr" if is_pr else "issue"
    return Shell.check(
        f"gh {kind} edit {number} --repo {shlex.quote(repo)} "
        f"--add-label {EXTERNAL_LABEL}",
        verbose=True,
    )


def label_external_contributors(days: int, backfill: bool) -> bool:
    repo = Info().repo_name
    members = fetch_org_members()
    print(f"ClickHouse organization has {len(members)} members")

    if backfill:
        since = None
        cutoff = None
        print("Backfill mode: scanning the entire issue and pull-request history")
    else:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=days
        )
        since = cutoff
        print(f"Scanning issues and pull requests created since {cutoff.isoformat()}")

    items = fetch_issues_and_prs(repo, since=since)
    print(f"Fetched {len(items)} item(s)")

    labeled = []
    for item in items:
        number = item["number"]
        is_pr = "pull_request" in item
        author = (item.get("user") or {}).get("login", "")
        created = _parse_iso8601(item["created_at"])
        if cutoff is not None and created < cutoff:
            continue
        if any(label["name"] == EXTERNAL_LABEL for label in item.get("labels", [])):
            continue
        if author.lower() in members or author.lower() in INTERNAL_BOTS:
            continue
        kind = "PR" if is_pr else "issue"
        print(f"Labeling {kind} #{number} by external author '{author}'")
        if not add_external_label(repo, number, is_pr):
            print(f"ERROR: failed to label #{number}", file=sys.stderr)
            return False
        labeled.append(number)

    print(f"Labeled {len(labeled)} item(s) as '{EXTERNAL_LABEL}': {labeled}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"scan items created within this many days (default {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="backfill",
        help="backfill: scan the entire history regardless of creation date",
    )
    args = parser.parse_args()

    Result.from_commands_run(
        name="Label external contributors",
        command=lambda: label_external_contributors(args.days, args.backfill),
    ).complete_job()
