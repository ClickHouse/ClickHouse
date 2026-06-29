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

When an original PR resolves issues -- taken from GitHub's "Development" links
(`closingIssuesReferences`), i.e. the issues it closes -- the same version-info
section is written into each such issue's body, so a reader of the issue sees
which release the fix shipped in. The section is the same delimited, bot-owned
block used in PR bodies (with an added `Resolved by` line), upserted the same
way, so it is idempotent: written once and re-edited only when the version info
changes.

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
require that the repo's CI populates `version_history` in the CIDB; reads use
`CIDBCluster`, which resolves the same `Settings.SECRET_CI_DB_*` credentials
that `version_log.py` writes with (the public `clickhouse-test-stat-*` params
or the private `PRIVATE_CI_DB_*` secrets), so the workflow must declare them.
"""

from __future__ import annotations

import argparse
import logging
import re
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

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
# mirrors cherry_pick.py:ReleaseBranch.bp_branch. Group 1 is the release (which
# may itself contain slashes, e.g. `release/26.5`), group 2 the original number.
BACKPORT_BRANCH_RE = re.compile(r"^backport/(.+)/(\d+)$")

# Mirrors tests/ci/pr_info.py:Labels -- a PR carrying any of these has (or is
# meant to have) backports, so it is worth scanning release branches for them.
BACKPORT_LABELS = {
    "pr-backports-created",
    "pr-backport",
    "pr-must-backport",
    "pr-must-backport-force",
}
VERSION_BACKPORT_LABEL_RE = re.compile(r"^v\d+\.\d+-must-backport$")

# PRs carrying any of these labels are skipped entirely. These are the
# automated periodic upstream-sync PRs in the private fork ("Automatic
# synchronization with upstream"); they are not real changes that ship in a
# release, so they get no version-info section.
IGNORE_LABELS = {"pr-periodic-sync-upstream"}


def has_ignore_label(label_names: List[str]) -> bool:
    return any(name in IGNORE_LABELS for name in label_names)


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


def render_issue_section(pr_number: int, section_body: str) -> str:
    """Render the version-info section written into a resolved issue's body.

    Same inner markdown as the PR-body section, but with a `Resolved by` item
    added as the first entry of the list (right after the `### Version info`
    header). The delimiters are added by `upsert_section`, exactly as for PRs.
    """
    header, _, rest = section_body.partition("\n")
    inner = f"{header}\n- Resolved by: #{pr_number}"
    if rest:
        inner += f"\n{rest}"
    return inner


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
        # Two blank lines before the block to visually separate the version-info
        # section from the preceding PR/issue description.
        body += "\n\n"
    return body + block


def original_pr_number_from_backport_ref(head_ref: str) -> Optional[int]:
    """Extract the original PR number from a `backport/<release>/<number>` ref."""
    match = BACKPORT_BRANCH_RE.match(head_ref)
    return int(match.group(2)) if match else None


def release_from_backport_ref(head_ref: str) -> Optional[str]:
    """Extract the release branch from a `backport/<release>/<number>` ref."""
    match = BACKPORT_BRANCH_RE.match(head_ref)
    return match.group(1) if match else None


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

    PRs carrying an ignore label (see ``IGNORE_LABELS``) are dropped entirely.
    """
    backport_numbers: Set[int] = set()
    original_numbers: Set[int] = set()
    need_scan: Set[int] = set()
    for pr in prs:
        if has_ignore_label(pr.label_names):
            continue
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

    def __init__(self, cidb) -> None:
        # `cidb` is a CIDBCluster; it resolves the CIDB credentials from the
        # workflow secrets (Settings.SECRET_CI_DB_*), which map to the public
        # `clickhouse-test-stat-*` params or the private `PRIVATE_CI_DB_*`
        # secrets -- the same cluster `version_log.py` writes `version_history`
        # to, so reads and writes always agree.
        self.cidb = cidb
        self._cache: Dict[str, Optional[str]] = {}
        # Guards `_cache` so the lookup can be shared by parallel backport scans.
        self._lock = threading.Lock()

    def version_for_commit(self, sha: Optional[str]) -> Optional[str]:
        # `do_select_query` does not bind parameters, so only a real git oid
        # (hex) is ever interpolated into the query text.
        if not sha or not re.fullmatch(r"[0-9a-fA-F]+", sha):
            return None
        with self._lock:
            if sha in self._cache:
                return self._cache[sha]
        # The query runs outside the lock so concurrent lookups for different
        # commits do not serialize; a duplicate query for the same sha is rare
        # and harmless.
        text = self.cidb.do_select_query(
            "SELECT version FROM version_history "
            f"WHERE commit_sha = '{sha}' "
            "ORDER BY check_start_time DESC LIMIT 1 "
            "FORMAT TabSeparated",
            db_name=CIDB_DATABASE,
        )
        version = (text or "").strip() or None
        with self._lock:
            self._cache[sha] = version
        return version


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
    gh,
    repo_name: str,
    numbers: List[int],
    release_branches: List[str],
    version_history: VersionHistory,
) -> Tuple[Dict[int, List[str]], Set[int]]:
    """Resolve, for each original PR number, the versions its merged backports
    shipped in by looking up `backport/<release>/<number>` on every release.

    The lookups for all ``(number, release)`` pairs are resolved with a few
    batched GraphQL requests rather than one REST request per pair, which keeps
    a wide lookback from tripping GitHub's secondary rate limit. Returns
    ``({number: [versions, newest first]}, failed_numbers)``; on a hard query
    failure every scanned original is reported failed so the caller skips it
    (fail closed -- never write a partial `Backported to` list).
    """
    if not numbers or not release_branches:
        return {}, set()

    pairs = [(number, release) for number in numbers for release in release_branches]
    head_refs = [f"backport/{release}/{number}" for number, release in pairs]
    try:
        merge_commits = gh.get_backport_merge_commits(repo_name, head_refs)
    except Exception as ex:  # pylint: disable=broad-except
        logging.error(
            "Backport scan failed; %s originals will be reconciled next run: %s",
            len(numbers),
            ex,
        )
        return {}, set(numbers)

    by_number: Dict[int, List[str]] = defaultdict(list)
    for number, release in pairs:
        oid = merge_commits.get(f"backport/{release}/{number}")
        if not oid:
            continue
        version = version_history.version_for_commit(oid)
        if version:
            by_number[number].append(version)

    return (
        {
            number: sorted(set(versions), key=version_key, reverse=True)
            for number, versions in by_number.items()
        },
        set(),
    )


def apply_issue_section(gh, repo, issue_number: int, section_body: str, dry_run: bool) -> bool:
    """Upsert the version-info section into an issue's body. Returns True if it
    changed. The issue is fetched live so the body reflects the current state."""
    issue = repo.get_issue(issue_number)
    new_body = upsert_section(issue.body, section_body)
    if new_body == (issue.body or ""):
        return False
    if dry_run:
        print(
            f"DRY RUN: would update issue #{issue_number} version info:\n"
            f"{section_body}\n"
        )
        return True
    issue.edit(body=new_body)
    logging.info("Updated issue #%s version info", issue_number)
    return True


def update_linked_issues(gh, repo, info, section_body: str, dry_run: bool) -> int:
    """Write the version-info section into the body of every issue the PR resolves.

    The issues come from the PR's GitHub "Development" links
    (`closingIssuesReferences`), not from re-parsing the body. The section is
    idempotent via its markers: written once, edited only when the version info
    changes. A per-issue failure is logged and skipped so it is reconciled on
    the next run rather than aborting the whole PR.
    """
    count = 0
    issue_section = render_issue_section(info.number, section_body)
    for issue_number in info.closing_issue_numbers:
        try:
            if apply_issue_section(gh, repo, issue_number, issue_section, dry_run):
                count += 1
                logging.info(
                    "Updated version info on issue #%s (resolved by PR #%s)",
                    issue_number,
                    info.number,
                )
        except Exception as ex:  # pylint: disable=broad-except
            logging.error(
                "Failed to update version info on issue #%s (from PR #%s): %s",
                issue_number,
                info.number,
                ex,
            )
    return count


def update_original_pr(
    gh,
    repo,
    info,
    version_history: VersionHistory,
    backported: List[str],
    dry_run: bool,
) -> bool:
    """Set `Merged into` and `Backported to` on a merged original PR, and write
    the same info into the body of every issue the PR resolves."""
    if not info.merged:
        return False
    merged_into = version_history.version_for_commit(info.merge_commit_sha)

    if not merged_into and not backported:
        # Nothing known yet -- the post-merge build has likely not finished.
        return False
    section_body = render_section(merged_into, backported)
    changed = apply_section(gh, repo, info, section_body, dry_run)
    # Mirror the section into the body of the issues this PR resolved (its
    # "Development" links). Only PRs that actually close an issue pay for this.
    if info.closing_issue_numbers:
        update_linked_issues(gh, repo, info, section_body, dry_run)
    return changed


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
    return parser.parse_args()


def main() -> int:
    from ci.jobs.scripts.cidb_cluster import CIDBCluster
    from ci.praktika.info import Info
    from get_robot_token import get_best_robot_token
    from github_helper import GitHub

    args = parse_args()
    token = args.token or get_best_robot_token()

    # A manual run can widen the lookback via the `days` workflow_dispatch input;
    # it overrides the command's --days. Absent/empty (scheduled runs) -> --days.
    days = args.days
    dispatch_days = Info.get_workflow_input_value("days")
    if dispatch_days and dispatch_days.strip().isdigit():
        days = int(dispatch_days)
        logging.info("Using lookback from workflow input: %s days", days)

    gh = GitHub(token, per_page=100)
    repo = gh.get_repo(args.repo)
    version_history = VersionHistory(CIDBCluster())
    release_branches = [pr.head.ref for pr in gh.get_release_pulls(args.repo)]
    logging.info("Active release branches: %s", release_branches)

    now = datetime.now()
    since = now - timedelta(days=days)
    # Lightweight GraphQL fetch: a few batched requests for the whole window,
    # instead of one REST request per merged PR.
    merged_infos = gh.get_pulls_lightweight(
        query=f"type:pr repo:{args.repo} is:merged",
        merged=[since, now],
    )
    logging.info("Found %s merged PRs in the last %s days", len(merged_infos), days)

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
    # batched GraphQL request. A failure here must not abort the run -- any
    # original we could not fetch simply stays absent and is skipped below.
    missing = [n for n in original_numbers if n not in infos_by_number]
    if missing:
        try:
            infos_by_number.update(
                gh.get_pulls_lightweight_by_numbers(args.repo, missing)
            )
        except Exception as ex:  # pylint: disable=broad-except
            logging.error(
                "Failed to fetch %s referenced original PR(s); they will be "
                "skipped and reconciled next run: %s",
                len(missing),
                ex,
            )

    # Release branches to scan = the currently-open releases plus any release a
    # backport merged into this window. The latter catches releases whose
    # `release` PR is already closed (so they are not in `release_branches`) but
    # that still receive backports -- otherwise the original's `Backported to`
    # would miss those versions.
    scan_release_branches = set(release_branches)
    for number in backport_numbers:
        release = release_from_backport_ref(infos_by_number[number].head_ref)
        if release:
            scan_release_branches.add(release)

    # Resolve every scanned original's backports up front via batched GraphQL.
    # `scan_failed` holds originals whose scan errored; they are skipped below so
    # we never write a partial list.
    backported_by_number, scan_failed = scan_backport_versions(
        gh,
        args.repo,
        sorted(need_scan),
        sorted(scan_release_branches),
        version_history,
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
        if has_ignore_label(info.label_names):
            # An on-demand original (pulled in by a backport) may still carry an
            # ignore label; `partition_merged_prs` only saw the in-window PRs.
            continue
        if number in scan_failed:
            logging.error("Skipping PR #%s: backport scan incomplete", number)
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
