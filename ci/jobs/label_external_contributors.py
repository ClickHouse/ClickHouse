"""Nightly job: label issues and pull requests from external contributors.

Adds the `external` label to items whose author is not a ClickHouse
organization member. Scans the last `--days` days (default 3); `--all`
backfills the whole history.
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

EXTERNAL_LABEL = "external"

INTERNAL_BOTS = {"groeneai", "oranjeai", "clickgapai", "actueleai", "snelheidai"}


def _member_logins(path: str) -> set:
    out = Shell.get_output(
        f"gh api {path} --paginate --jq '.[].login'", verbose=True, strict=True
    )
    return {line.strip().lower() for line in out.splitlines() if line.strip()}


def fetch_org_members(org: str) -> set:
    members = _member_logins(f"orgs/{shlex.quote(org)}/members")
    public = _member_logins(f"orgs/{shlex.quote(org)}/public_members")
    assert len(members) > len(public), (
        f"GitHub token cannot read private {org} membership: orgs/{org}/members "
        f"returned {len(members)} logins, no more than the public list "
        f"({len(public)}). The workflow must mint its token from the trusted "
        f"lambda, whose GitHub App has the organization 'Members: Read' "
        f"permission; otherwise private members are mislabeled as external."
    )
    return members


def _is_internal(login: str, user_type: str, members: set) -> bool:
    if user_type == "Bot":
        return True
    low = login.lower()
    return low in INTERNAL_BOTS or low.startswith("robot-") or low in members


def _parse_iso8601(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_issues_and_prs(repo: str, since: datetime.datetime = None) -> list:
    url = f"repos/{repo}/issues?state=all&per_page=100"
    if since is not None:
        url += f"&since={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    out = Shell.get_output(
        f"gh api --paginate --slurp {shlex.quote(url)}",
        verbose=True,
        strict=True,
    )
    return [item for page in json.loads(out) for item in page]


def add_external_label(repo: str, number: int, is_pr: bool) -> bool:
    kind = "pr" if is_pr else "issue"
    return Shell.check(
        f"gh {kind} edit {number} --repo {shlex.quote(repo)} "
        f"--add-label {EXTERNAL_LABEL}",
        verbose=True,
    )


def label_external_contributors(days: int, backfill: bool) -> bool:
    repo = Info().repo_name
    org = repo.split("/")[0]
    members = fetch_org_members(org)
    print(f"{org} organization has {len(members)} members")

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
    failed = []
    for item in items:
        number = item["number"]
        is_pr = "pull_request" in item
        user = item.get("user") or {}
        author = user.get("login", "")
        created = _parse_iso8601(item["created_at"])
        if cutoff is not None and created < cutoff:
            continue
        if any(label["name"] == EXTERNAL_LABEL for label in item.get("labels", [])):
            continue
        if _is_internal(author, user.get("type", ""), members):
            continue
        kind = "PR" if is_pr else "issue"
        print(f"Labeling {kind} #{number} by external author '{author}'")
        if add_external_label(repo, number, is_pr):
            labeled.append(number)
        else:
            print(f"ERROR: failed to label #{number}", file=sys.stderr)
            failed.append(number)

    print(f"Labeled {len(labeled)} item(s) as '{EXTERNAL_LABEL}': {labeled}")
    if failed:
        print(f"ERROR: failed to label {len(failed)} item(s): {failed}", file=sys.stderr)
        return False
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
