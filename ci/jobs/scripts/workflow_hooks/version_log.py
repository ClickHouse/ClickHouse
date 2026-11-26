from datetime import datetime
from typing import List

from praktika.info import Info
from praktika.utils import Shell

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_version import CHVersion

# How many first-parent commits before `HEAD` each run checks for a missing
# `version_history` row (see `_missing_commits`). Batched pushes skip at most a
# handful of commits, so this leaves a wide margin for runs that were skipped or
# cancelled entirely, while keeping the `IN (...)` list of the lookup query
# (sent in the URL) small.
BACKFILL_WINDOW_COMMITS = 100


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


def _missing_commits(head_sha: str) -> List[str]:
    """The first-parent commits among the `BACKFILL_WINDOW_COMMITS` preceding
    `head_sha` that have no branch-build row in `version_history`, oldest first.
    `head_sha` itself is excluded: the normal path below logs it.

    A GitHub `push` webhook fires once per push, not once per commit, so when
    the merge queue lands several PRs in one push, only `head_sha` gets a
    `push`-triggered CI run -- the earlier commits never run this hook and are
    otherwise permanently missing from `version_history` (see
    `ci/tools/pr_version_info.py`, which reads that table by merge commit).
    Backfilling them here is cheap: a version + a DB row, not a rebuild.

    The missing commits are found by set difference against the rows already
    recorded for the window, not by walking from the latest recorded row: the
    latest row by `check_start_time` is ambiguous (second resolution, reruns of
    old workflows), and a set difference re-examines the whole window on every
    run, so a gap left by a failed or partial backfill is picked up by the next
    run. The lookup deliberately ignores `git_ref`: a commit already logged
    under another branch (e.g. `master` history inherited by a new release
    branch) has the same version and needs no second row. PR builds are
    excluded, since they log a pinned tweak for an ephemeral merge commit.

    Returns an empty list when the rows cannot be read: without knowing what is
    recorded, backfilling would insert duplicates of the whole window."""
    window = [
        c
        for c in Shell.get_output(
            f"git rev-list --first-parent --max-count={BACKFILL_WINDOW_COMMITS} {head_sha}^",
            strict=True,
        ).split("\n")
        if c
    ]
    if not window:
        return []
    in_list = ", ".join(f"'{c}'" for c in window)
    text = CIDBCluster().do_select_query(
        "SELECT DISTINCT commit_sha FROM version_history "
        f"WHERE commit_sha IN ({in_list}) AND pull_request_number = 0 "
        "FORMAT TabSeparated"
    )
    if text is None:
        print("Could not read recorded commits from version_history, skip backfill")
        return []
    recorded = set(text.split())
    return [c for c in reversed(window) if c not in recorded]


def _version_history_row(info, commit_sha: str, version: CHVersion) -> dict:
    return {
        "check_start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pull_request_number": info.pr_number,
        "pull_request_url": info.pr_url,
        "commit_sha": commit_sha,
        "commit_url": info.commit_url.replace(info.sha, commit_sha),
        "parent_commits_sha": Shell.get_output(
            f"git log --format=%P -n 1 {commit_sha}"
        ).split(" "),
        "version": version.string,
        "git_ref": info.git_branch,
    }


def _backfill_skipped_commits(info: Info, cidb: CIDBCluster) -> None:
    """Log a `version_history` row for every commit a prior batched `push`
    skipped (see `_missing_commits`). Best-effort: any failure here is logged
    and swallowed so it can never block logging `HEAD`'s own row below. A commit
    that fails to be logged stays in the window and is retried by later runs."""
    if info.pr_number != 0:
        return  # Only meaningful for a push to a real branch, not a PR build.
    try:
        for commit in _missing_commits(info.sha):
            data = _version_history_row(
                info, commit, CHVersion.get_version_at_commit(commit)
            )
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

    _backfill_skipped_commits(info, cidb)

    data = _version_history_row(info, info.sha, version)
    print(f"Update version log: [{data}]")
    cidb.insert_json(table="version_history", json_str=data)
    # stores actual version data in pipline storage, to be used by jobs that need it
    version.store_version_data_in_ci_pipeline()


if __name__ == "__main__":
    _add_build_to_version_history()
