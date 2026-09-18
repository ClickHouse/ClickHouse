from datetime import datetime
from typing import List, Optional

from praktika.info import Info
from praktika.utils import Shell

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_version import (
    CHVersion,
    tweak_at_commit,
    version_file_githash,
)

# How many commits a single run will backfill for a previously-skipped `push`
# batch (see `_missing_commits`). Bounds the extra git/CIDB work if the log has
# been broken for a while; anything beyond this is left for a later run to pick
# up once it becomes the new "last known" gap.
MAX_BACKFILL_COMMITS = 200


def _build_version(info):
    """The build version recorded for this run.

    In a PR the tweak is pinned to 1. `HEAD` is the ephemeral merge commit,
    whose first-parent commit count diverges across close/reopen and re-runs of
    the same PR as `master` advances. Artifacts are keyed by the head SHA, so an
    unpinned tweak would store diverging version strings -- both in the
    `version_history` log and in the packages built from the pipeline kv data --
    under one artifact prefix. The tweak is meaningless in a PR anyway."""
    version = CHVersion.get_current_version(no_strict=True)
    if info.pr_number != 0:
        version = version.with_tweak(1)
    return version


def _last_known_commit(git_ref: str) -> Optional[str]:
    """The most recently recorded `commit_sha` in `version_history` for `git_ref`.

    Returns `None` (rather than raising) on any CIDB failure -- a backfill is a
    nice-to-have, not something that may block logging `HEAD`'s own row."""
    try:
        text = CIDBCluster().do_select_query(
            "SELECT commit_sha FROM version_history "
            f"WHERE git_ref = '{git_ref}' "
            "ORDER BY check_start_time DESC LIMIT 1 "
            "FORMAT TabSeparated"
        )
    except Exception as ex:  # pylint: disable=broad-except
        print(f"Could not look up the last recorded commit for backfill: {ex}")
        return None
    return (text or "").strip() or None


def _missing_commits(last_known_sha: Optional[str], head_sha: str) -> List[str]:
    """First-parent commits strictly between `last_known_sha` and `head_sha`
    (excluding `head_sha` itself, which gets logged by the normal path below),
    oldest first.

    A GitHub `push` webhook fires once per push, not once per commit, so when
    the merge queue lands several PRs in one push, only `head_sha` gets a
    `push`-triggered CI run -- the earlier commits never run this hook and are
    otherwise permanently missing from `version_history` (see
    `tests/ci/pr_version_info.py`, which reads that table and can only skip a
    commit it never finds). Backfilling them here is cheap: a version + a DB
    row, not a rebuild.

    Empty when there is nothing to backfill (the common case: `last_known_sha`
    is `head_sha`'s immediate first parent), when `last_known_sha` is unknown to
    this checkout (a shallow/partial clone, or the very first run after a
    version bump), or when `last_known_sha` could not be resolved at all.
    """
    if not last_known_sha or last_known_sha == head_sha:
        return []
    if not Shell.check(f"git cat-file -e {last_known_sha}^{{commit}} 2>/dev/null"):
        return []
    commits = [
        c
        for c in Shell.get_output(
            f"git rev-list --first-parent --reverse {last_known_sha}..{head_sha}"
        ).split("\n")
        if c and c != head_sha
    ]
    # Keep the commits nearest HEAD: they are the ones a subsequent run would
    # otherwise treat as the new "last known" gap boundary.
    return commits[-MAX_BACKFILL_COMMITS:]


def _version_history_row(info, commit_sha: str, version: CHVersion) -> dict:
    return {
        "check_start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pull_request_number": info.pr_number,
        "pull_request_url": info.pr_url,
        "commit_sha": commit_sha,
        "commit_url": info.commit_url,
        "parent_commits_sha": Shell.get_output(
            f"git log --format=%P -n 1 {commit_sha}"
        ).split(" "),
        "version": version.string,
        "git_ref": info.git_branch,
    }


def _backfill_skipped_commits(
    info: Info, cidb: CIDBCluster, version: CHVersion
) -> None:
    """Log a `version_history` row for every commit a prior batched `push`
    skipped (see `_missing_commits`). Best-effort: any failure here is logged
    and swallowed so it can never block logging `HEAD`'s own row below --
    reconciliation just picks up a wider gap on a later run."""
    if info.pr_number != 0:
        return  # Only meaningful for a push to a real branch, not a PR build.
    try:
        last_known_sha = _last_known_commit(info.git_branch)
        baseline_githash = version_file_githash()
        for commit in _missing_commits(last_known_sha, info.sha):
            backfilled = CHVersion(
                version.major,
                version.minor,
                version.patch,
                version.revision,
                tweak=tweak_at_commit(baseline_githash, commit),
                version_type=version.version_type,
            )
            data = _version_history_row(info, commit, backfilled)
            print(
                f"Backfilling version log for a commit skipped by a batched push: [{data}]"
            )
            cidb.insert_json(table="version_history", json_str=data)
    except Exception as ex:  # pylint: disable=broad-except
        print(f"Skipping version-history backfill: {ex}")


def _add_build_to_version_history():
    info = Info()
    Shell.check(
        f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin {info.git_branch} ||:"
    )
    version = _build_version(info)
    cidb = CIDBCluster()

    _backfill_skipped_commits(info, cidb, version)

    data = _version_history_row(info, info.sha, version)
    print(f"Update version log: [{data}]")
    cidb.insert_json(table="version_history", json_str=data)
    # stores actual version data in pipline storage, to be used by jobs that need it
    version.store_version_data_in_ci_pipeline()


if __name__ == "__main__":
    _add_build_to_version_history()
