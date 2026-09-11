"""Nightly job: label issues and pull requests from external contributors.

Attaches the `external` label to issues and pull requests whose author is not a
member of the ClickHouse GitHub organization, so external contributions are
easy to filter and triage. By default it scans items created in the last
`--days` days (3, matching the nightly cadence with margin); pass `--all` to
backfill the entire history in one run.

Everything the feature needs lives in this one script: the internal/external
classification, the bot allowlist, and the labeling of both issues and pull
requests. The repository issues endpoint returns pull requests too (each one
carries a `pull_request` field), so a single scan covers both.

Membership is read from the `author_association` GitHub attaches to every item,
not from the `orgs/.../members` list. The list only returns members visible to
the token (private memberships are omitted, and `gh`'s `--cache`/`--paginate`
combination can return just the first page), which mislabeled real members such
as `leshikus` and the member-robots. `author_association` is computed
server-side, sees private membership, and is already in the payload - no extra
API calls.

Automation accounts operated by ClickHouse that are not organization members
(e.g. the `*ai` PR bots) still show up as external contributors, so they are
excluded via INTERNAL_BOTS / the `robot-` prefix.
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

# `author_association` values that mean the author is inside the project: an
# organization owner or member, or a repository collaborator (invited, with
# repository access - trusted, not an outside contributor). Everything else
# (CONTRIBUTOR, FIRST_TIME_CONTRIBUTOR, FIRST_TIMER, NONE) is external.
INTERNAL_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}

# Automation accounts operated by ClickHouse that are not organization members
# but must not be labeled `external`. The member-robots (robot-clickhouse-ci-2,
# robot-ch-test-poll4, ...) already classify as MEMBER; this covers the ones
# that do not, and the `robot-` prefix guards any future robot account that has
# not been added to the organization yet.
INTERNAL_BOTS = {"groeneai", "oranjeai", "clickgapai", "actueleai"}


def _is_internal(author: str, association: str, user_type: str) -> bool:
    """Whether the author must not be labeled external: an org member/owner or
    repository collaborator, a GitHub App bot (`user.type == "Bot"`, e.g.
    `clickhouse-gh[bot]`, `mintlify[bot]`), or a ClickHouse automation account.

    Bots are never external contributors regardless of their association, and a
    GitHub App reports `type: "Bot"`, so that one check covers every App bot
    without an allowlist. The `*ai` PR bots are ordinary user accounts
    (`type: "User"`), so they still need INTERNAL_BOTS."""
    if user_type == "Bot":
        return True
    if association in INTERNAL_ASSOCIATIONS:
        return True
    login = author.lower()
    return login in INTERNAL_BOTS or login.startswith("robot-")


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
        user = item.get("user") or {}
        author = user.get("login", "")
        association = item.get("author_association", "")
        created = _parse_iso8601(item["created_at"])
        if cutoff is not None and created < cutoff:
            continue
        if any(label["name"] == EXTERNAL_LABEL for label in item.get("labels", [])):
            continue
        if _is_internal(author, association, user.get("type", "")):
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
