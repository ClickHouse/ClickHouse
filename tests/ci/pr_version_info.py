#!/usr/bin/env python3
"""
Maintain a "Version info" section in ClickHouse pull-request descriptions.

For every merged PR we look up the release version it shipped in and write a
delimited, bot-owned section into the PR body, e.g.:

    <!-- ch-version-info:start -->
    ### Version info
    - Merged into: `26.6.1.1`
    - Backported to: `25.12.1.100`, `25.8.1.200`
    <!-- ch-version-info:end -->

The version is taken from the CIDB `version_history` table, populated per build
by `ci/jobs/scripts/workflow_hooks/version_log.py`. Each build stores the
commit it ran on (`commit_sha`) and the computed `version`. A PR's merge commit
(`PullRequest.merge_commit_sha`) equals that `commit_sha` for the post-merge
build, so a single lookup maps PR -> version.

For an original PR (merged to the default branch) we additionally scan the
active release branches for its backport PRs (`backport/<release>/<pr_number>`)
and list the versions those backports shipped in.

The job is idempotent: it re-derives everything from GitHub + `version_history`
on each run and only edits a PR when the section content actually changes. A
missing version (the post-merge build has not finished yet, or CIDB is
unreachable) means we skip the PR and reconcile on the next run -- we never
write a guessed/placeholder version.

Backports are normally merged *later* than the original PR, often after the
original has left the lookback window. This is handled: a merged backport PR
pulls its original PR back in (by the `backport/<release>/<number>` head ref)
and the original's `Backported to` list is recomputed by an authoritative scan
of all active release branches -- see `partition_merged_prs`. So every backport
merge, whenever it happens, refreshes the original PR.

Works in both the public and private repositories. The repo is taken from
`GITHUB_REPOSITORY`; release branches are read from the open `release` PRs, so
the private repo's `release/<x.y>` head refs (and the resulting
`backport/release/<x.y>/<number>` branches) are matched the same way as the
public repo's bare `<x.y>` names. Versions are matched by `commit_sha`, which is
unique per merge commit per repo, so there is no cross-repo mismatch. It does
require that the repo's CI populates `version_history` in the CIDB reachable via
the `clickhouse-test-stat-*` SSM parameters.
"""

from __future__ import annotations

import argparse
import logging
import re
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

# How many release-branch backport lookups to run concurrently. These are the
# only per-PR REST requests left after the lightweight GraphQL window fetch.
DEFAULT_SCAN_THREADS = 12

# Heavy imports (github, clickhouse_helper, ...) are done lazily inside main() so
# that the pure helpers below stay importable for unit tests without PyGithub.

SECTION_START = "<!-- ch-version-info:start -->"
SECTION_END = "<!-- ch-version-info:end -->"
SECTION_RE = re.compile(
    re.escape(SECTION_START) + r".*?" + re.escape(SECTION_END), re.DOTALL
)

# CIDB database holding the `version_history` table (ci/settings/settings.py:
# CI_DB_DB_NAME).
CIDB_DATABASE = "default"

# How far back to look for merged PRs to reconcile, in days.
DEFAULT_LOOKBACK_DAYS = 30

# Backport branches are named `backport/<release>/<original_pr_number>`; this
# mirrors cherry_pick.py:ReleaseBranch.bp_branch.
BACKPORT_BRANCH_RE = re.compile(r"^backport/.+/(\d+)$")

# Mirrors tests/ci/pr_info.py:Labels -- a PR carrying any of these has (or is
# meant to have) backports, so it is worth scanning release branches for them.
BACKPORT_LABELS = {
    "pr-backports-created",
    "pr-backport",
    "pr-must-backport",
    "pr-must-backport-force",
}
VERSION_BACKPORT_LABEL_RE = re.compile(r"^v\d+\.\d+-must-backport$")


def version_key(version: str) -> Tuple[int, ...]:
    """Sort key for version strings like ``26.6.1.1``."""
    return tuple(int(part) for part in version.split(".") if part.isdigit())


def render_section(merged_into: Optional[str], backported_to: List[str]) -> str:
    """Render the inner markdown of the version-info section (no delimiters)."""
    lines = ["### Version info"]
    if merged_into:
        lines.append(f"- Merged into: `{merged_into}`")
    if backported_to:
        versions = ", ".join(f"`{v}`" for v in backported_to)
        lines.append(f"- Backported to: {versions}")
    return "\n".join(lines)


def upsert_section(body: Optional[str], section_body: str) -> str:
    """Insert or replace the bot-owned block delimited by the markers.

    ``section_body`` is the inner markdown without the delimiters. Returns the
    new PR body. Pure function -- the core of the bot's idempotency.
    """
    body = body or ""
    block = f"{SECTION_START}\n{section_body}\n{SECTION_END}"
    if SECTION_RE.search(body):
        # Use a function replacement so backslashes/group refs in `block` are
        # treated literally.
        return SECTION_RE.sub(lambda _match: block, body)
    if body and not body.endswith("\n"):
        body += "\n"
    if body:
        body += "\n"
    return body + block


def original_pr_number_from_backport_ref(head_ref: str) -> Optional[int]:
    """Extract the original PR number from a `backport/<release>/<number>` ref."""
    match = BACKPORT_BRANCH_RE.match(head_ref)
    return int(match.group(1)) if match else None


def has_backport_label(label_names: List[str]) -> bool:
    return any(
        name in BACKPORT_LABELS or VERSION_BACKPORT_LABEL_RE.match(name)
        for name in label_names
    )


class MergedPR(NamedTuple):
    """Minimal view of a merged PR used for classification (test-friendly)."""

    number: int
    head_ref: str
    base_ref: str
    label_names: List[str]


def partition_merged_prs(
    prs: Iterable[MergedPR], default_branch: str
) -> Tuple[Set[int], Set[int], Set[int]]:
    """Classify merged PRs into backports and originals.

    Returns ``(backport_numbers, original_numbers, need_scan)``:
      * ``backport_numbers`` -- PRs whose head is `backport/<release>/<number>`;
        each gets its own `Merged into` line.
      * ``original_numbers`` -- PRs merged into ``default_branch``, plus the
        originals referenced by any backport above (even if those originals are
        not in the merged set themselves -- the "backport merged later" case).
      * ``need_scan`` -- originals worth scanning the release branches for
        backports: any original referenced by a backport, plus originals that
        carry a backport label.
    """
    backport_numbers: Set[int] = set()
    original_numbers: Set[int] = set()
    need_scan: Set[int] = set()
    for pr in prs:
        original_number = original_pr_number_from_backport_ref(pr.head_ref)
        if original_number is not None:
            backport_numbers.add(pr.number)
            original_numbers.add(original_number)
            need_scan.add(original_number)
        elif pr.base_ref == default_branch:
            original_numbers.add(pr.number)
            if has_backport_label(pr.label_names):
                need_scan.add(pr.number)
    return backport_numbers, original_numbers, need_scan


class VersionHistory:
    """Reads versions from the CIDB `version_history` table, with a SHA cache."""

    def __init__(self, ch_helper) -> None:
        self.ch = ch_helper
        self._cache: Dict[str, Optional[str]] = {}
        # Guards `_cache` so the lookup can be shared by parallel backport scans.
        self._lock = threading.Lock()

    def version_for_commit(self, sha: Optional[str]) -> Optional[str]:
        if not sha:
            return None
        with self._lock:
            if sha in self._cache:
                return self._cache[sha]
        # The query runs outside the lock so concurrent lookups for different
        # commits do not serialize; a duplicate query for the same sha is rare
        # and harmless.
        rows = self.ch.select_json_each_row(
            CIDB_DATABASE,
            "SELECT version FROM version_history "
            "WHERE commit_sha = {sha:String} "
            "ORDER BY check_start_time DESC LIMIT 1",
            query_params={"sha": sha},
        )
        version = rows[0]["version"] if rows else None
        with self._lock:
            self._cache[sha] = version
        return version


def get_backport_pr(repo, owner: str, release: str, number: int):
    """Return the (merged, if any) backport PR for ``number`` on ``release``."""
    head = f"{owner}:backport/{release}/{number}"
    candidates = list(repo.get_pulls(state="all", head=head))
    merged = [pr for pr in candidates if pr.merged_at is not None]
    if merged:
        return merged[0]
    return candidates[0] if candidates else None


def apply_section(gh, repo, info, section_body: str, dry_run: bool) -> bool:
    """Write the section into the PR if it changed. Returns True if changed.

    ``info`` is the lightweight GraphQL record; a full ``PullRequest`` (needed to
    call ``edit``) is fetched only when an edit is actually performed.
    """
    new_body = upsert_section(info.body, section_body)
    if new_body == (info.body or ""):
        return False
    if dry_run:
        print(
            f"DRY RUN: would update PR #{info.number} version info:\n{section_body}\n"
        )
        return True
    pr = gh.get_pull_cached(repo, info.number)
    pr.edit(body=new_body)
    logging.info("Updated PR #%s version info", info.number)
    return True


def update_backport_pr(
    gh, repo, info, version_history: VersionHistory, dry_run: bool
) -> bool:
    """Set the `Merged into` line on a merged backport PR."""
    version = version_history.version_for_commit(info.merge_commit_sha)
    if not version:
        return False
    return apply_section(gh, repo, info, render_section(version, []), dry_run)


def scan_backport_versions(
    repo,
    owner: str,
    numbers: List[int],
    release_branches: List[str],
    version_history: VersionHistory,
    threads: int,
) -> Dict[int, List[str]]:
    """Resolve, for each original PR number, the versions its merged backports
    shipped in by scanning every active release branch.

    Each ``(number, release)`` pair is an independent REST lookup, so they run
    concurrently. Returns ``{number: [versions, newest first]}``.
    """
    tasks = [(number, release) for number in numbers for release in release_branches]
    by_number: Dict[int, List[str]] = defaultdict(list)
    if not tasks:
        return {}

    def work(task: Tuple[int, str]) -> Tuple[int, Optional[str]]:
        number, release = task
        backport_pr = get_backport_pr(repo, owner, release, number)
        if backport_pr is None or backport_pr.merged_at is None:
            return number, None
        return number, version_history.version_for_commit(backport_pr.merge_commit_sha)

    with ThreadPoolExecutor(max_workers=max(1, threads)) as executor:
        for number, version in executor.map(work, tasks):
            if version:
                by_number[number].append(version)

    return {
        number: sorted(set(versions), key=version_key, reverse=True)
        for number, versions in by_number.items()
    }


def update_original_pr(
    gh,
    repo,
    info,
    version_history: VersionHistory,
    backported: List[str],
    dry_run: bool,
) -> bool:
    """Set `Merged into` and `Backported to` on a merged original PR."""
    if not info.merged:
        return False
    merged_into = version_history.version_for_commit(info.merge_commit_sha)

    if not merged_into and not backported:
        # Nothing known yet -- the post-merge build has likely not finished.
        return False
    return apply_section(
        gh, repo, info, render_section(merged_into, backported), dry_run
    )


def parse_args() -> argparse.Namespace:
    from env_helper import GITHUB_REPOSITORY

    parser = argparse.ArgumentParser(
        "Maintain a version-info section in PR descriptions",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--token", help="github token, taken from SSM if not set")
    parser.add_argument("--repo", default=GITHUB_REPOSITORY, help="repo owner/name")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="how far back to look for merged PRs",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="do not edit any PR, only print"
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=DEFAULT_SCAN_THREADS,
        help="concurrent backport-branch lookups",
    )
    return parser.parse_args()


def main() -> int:
    from clickhouse_helper import ClickHouseHelper
    from get_robot_token import get_best_robot_token
    from github_helper import GitHub

    args = parse_args()
    token = args.token or get_best_robot_token()
    owner = args.repo.split("/")[0]

    gh = GitHub(token, per_page=100)
    repo = gh.get_repo(args.repo)
    version_history = VersionHistory(ClickHouseHelper())
    release_branches = [pr.head.ref for pr in gh.get_release_pulls(args.repo)]
    logging.info("Active release branches: %s", release_branches)

    now = datetime.now()
    since = now - timedelta(days=args.days)
    # Lightweight GraphQL fetch: a few batched requests for the whole window,
    # instead of one REST request per merged PR.
    merged_infos = gh.get_pulls_lightweight(
        query=f"type:pr repo:{args.repo} is:merged",
        merged=[since, now],
    )
    logging.info(
        "Found %s merged PRs in the last %s days", len(merged_infos), args.days
    )

    infos_by_number = {info.number: info for info in merged_infos}
    backport_numbers, original_numbers, need_scan = partition_merged_prs(
        (
            MergedPR(info.number, info.head_ref, info.base_ref, info.label_names)
            for info in merged_infos
        ),
        repo.default_branch,
    )

    # Originals referenced only by a backport (merged later, after the original
    # left the lookback window) are not in the window result; fetch them in one
    # batched GraphQL request.
    missing = [n for n in original_numbers if n not in infos_by_number]
    if missing:
        infos_by_number.update(gh.get_pulls_lightweight_by_numbers(args.repo, missing))

    # Resolve every scanned original's backports up front, in parallel -- the
    # only remaining per-PR REST traffic.
    backported_by_number = scan_backport_versions(
        repo, owner, sorted(need_scan), release_branches, version_history, args.threads
    )

    updated = 0
    for number in sorted(backport_numbers):
        info = infos_by_number[number]
        try:
            updated += update_backport_pr(gh, repo, info, version_history, args.dry_run)
        except Exception as ex:  # pylint: disable=broad-except
            logging.error("Failed to process backport PR #%s: %s", number, ex)

    for number in sorted(original_numbers):
        info = infos_by_number.get(number)
        if info is None:
            logging.error("Could not fetch PR #%s, skipping", number)
            continue
        try:
            updated += update_original_pr(
                gh,
                repo,
                info,
                version_history,
                backported_by_number.get(number, []),
                args.dry_run,
            )
        except Exception as ex:  # pylint: disable=broad-except
            logging.error("Failed to process PR #%s: %s", number, ex)

    logging.info("Done. Updated %s PR(s)", updated)
    return updated


if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)
    # Imported here (not at module top) so the pure helpers stay importable for
    # unit tests without the praktika package on the path.
    from ci.praktika.result import Result

    status = Result.Status.OK
    info = ""
    try:
        info = f"Updated {main()} PR(s)"
    except Exception as error:  # pylint: disable=broad-except
        status = Result.Status.FAIL
        info = f"ERROR: {error}"
        logging.exception("pr_version_info failed")
    # Emit a praktika Result so the job is reported as completed; a plain
    # command that exits 0 without a Result is otherwise treated as killed.
    Result.create_from(status=status, info=info).complete_job()
