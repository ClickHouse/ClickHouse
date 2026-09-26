import copy
import json
import os
import re

from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.jobs.scripts.settings_history import (
    SETTINGS_DECLARATION_FILES,
    parse_declaration_changes,
)
from ci.praktika.digest import Digest
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.utils import Shell

_COMMIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


def _is_commit_sha(value):
    """Whether `value` is a full commit id.

    `gh api -q` prints an absent field as an empty line and a present non-id field verbatim,
    both at exit code 0, so a successful read is not by itself a revision."""
    return bool(_COMMIT_SHA_RE.fullmatch(value or ""))


# How many first-parent master commits to record in `master_track_commits_sha`,
# and how many `/commits` pages the walk that reconstructs them may fetch. One
# page (100 entries) covers 100 first-parent commits only when master holds no
# merge commit at all; the budget leaves room for the side-branch commits the
# listing interleaves, and a walk that runs out of it returns the (shorter)
# chain it has instead of failing the whole hook.
MASTER_TRACK_COMMITS = 50
MASTER_TRACK_MAX_PAGES = 10


def _list_master_commits_page(anchor_sha):
    """One page of the commits reachable from `anchor_sha`, newest first.

    Each entry is `(sha, first_parent_sha)`; the first parent is empty for a
    root commit. This is NOT the first-parent chain: the listing interleaves
    merged PRs' side-branch commits, which is why `get_master_first_parent_commits`
    reconstructs the chain client-side from the parent shas."""
    raw = Shell.get_output(
        f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={anchor_sha}&per_page=100'"
        " -q '.[] | [.sha, (.parents[0].sha // \"\")] | @tsv'",
        verbose=True,
    )
    page = []
    for line in raw.splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0]:
            page.append((parts[0], parts[1]))
    return page


def get_master_first_parent_commits(
    anchor_sha, count, list_page=_list_master_commits_page
):
    """Up to `count` commits of the first-parent chain starting at `anchor_sha`, newest first.

    `repos/.../commits?sha=...` lists every commit reachable from the anchor,
    merged PRs' side-branch commits included, so the listing itself must not be
    taken for the master chain: consumers that walk `master_track_commits_sha`
    commit by commit (the perf `release_base` gate) would spend their window on
    commits that never had a master CI run, and a single merge of a long branch
    could push the previous actual master run out of the stored list entirely.

    The chain is therefore reconstructed by following `parents[0]`, re-anchoring
    each fetch at the first sha the walk has not seen yet - a listing always
    starts with its own anchor, so every fetch advances the walk. A listing that
    does not contain its own anchor means the fetch failed or returned something
    unusable: stop there and return the chain built so far rather than continue
    with a hole in it."""
    first_parent = {}
    chain = []
    wanted = anchor_sha
    pages = 0
    while True:
        while wanted in first_parent:
            chain.append(wanted)
            wanted = first_parent[wanted]
            if not wanted or len(chain) >= count:
                return chain
        if pages >= MASTER_TRACK_MAX_PAGES:
            print(
                f"WARNING: the master first-parent chain holds only {len(chain)} "
                f"commits after {MASTER_TRACK_MAX_PAGES} pages"
            )
            return chain
        pages += 1
        for sha, parent in list_page(wanted):
            first_parent.setdefault(sha, parent)
        if wanted not in first_parent:
            print(
                f"WARNING: the commit listing anchored at {wanted} does not "
                f"contain it - stopping the master first-parent walk"
            )
            return chain


def fetch_patch_and_file(repo_name, pr_number, path):
    """Return `(patch, file_lines)` for `path` in `pr_number`, or raise naming the cause.

    CI containers have no .git history, so both come from the GitHub API, and from the same
    file entry: `.contents_url` names the file at the very revision `.patch` was computed
    against. Reading the checked-out file instead would resolve the patch's new-file line
    numbers against the PR merged with its base, whose numbering can differ, attributing
    records to the wrong setting.

    The style check reports whichever message this raises, so the failure modes must stay
    distinguishable: a failed command, a `null` patch, and an entry the API never returned."""
    if pr_number <= 0:
        raise RuntimeError(
            "could not resolve the PR number for the settings-history diff"
        )
    # `.patch` is the unified diff for just this file (hunks only, no file header).
    # strict=True so a command failure is not laundered into an empty result, which the
    # checks below would then mislabel as the large-diff case.
    file_entry = GH.get_output_with_retries(
        f"gh api repos/{repo_name}/pulls/{pr_number}/files --paginate "
        f"--jq '.[] | select(.filename == \"{path}\") "
        "| {patch, contents_url}'",
        verbose=True,
        strict=True,
    )
    if not file_entry.strip():
        # rc=0 with no output: the jq `select` matched nothing, i.e. the API's file list does
        # not contain this file even though changed_files says it changed.
        raise RuntimeError(
            f"{path} is in changed_files but absent from the GitHub API file list for "
            f"PR {pr_number}"
        )
    file_entry = json.loads(file_entry)
    patch = file_entry["patch"] or ""
    if not patch.strip():
        # GitHub omits the per-file patch for very large diffs; `.patch` is then null.
        raise RuntimeError(
            f"no patch returned for changed file {path} "
            "(GitHub omits the patch for very large diffs)"
        )
    contents_url = file_entry["contents_url"]
    head_file = GH.get_output_with_retries(
        f'gh api -H "Accept: application/vnd.github.raw" "{contents_url}"',
        verbose=True,
        strict=True,
    )
    if not head_file.strip():
        raise RuntimeError(f"no content returned for {contents_url}")
    return patch, head_file.splitlines()


_FETCH_ERROR_MESSAGE_LIMIT = 500
# Elide the middle: the head window is the one that keeps the cause, because
# `GH.get_output_with_retries` puts it ahead of the API-controlled output. The tail window is
# only a hedge for arbitrary exception texts, not a guarantee for that helper's `err` field.
_FETCH_ERROR_MESSAGE_TAIL = 80


def settings_history_fetch_error_message(exc):
    """Bound the failure reason: the style check prints it on a public report page."""
    message = " ".join(str(exc).split())
    if len(message) <= _FETCH_ERROR_MESSAGE_LIMIT:
        return message
    marker = f"...(elided, full message {len(message)} chars)..."
    head = _FETCH_ERROR_MESSAGE_LIMIT - len(marker) - _FETCH_ERROR_MESSAGE_TAIL
    return message[:head] + marker + message[-_FETCH_ERROR_MESSAGE_TAIL :]


# Paths whose changed lines decide whether a PR is "small" for the purpose of
# skipping the stress tests, fuzzers and SQL suites (see `filter_job.py`): the product-code
# part of `build_digest_config.include_paths`, i.e. everything whose change ends
# up in the built server. Tests, docs and CI scripts do not count: only changes
# to the server itself can introduce the bugs those jobs look for.
#
# `contrib/` and `.gitmodules` are deliberately absent. A submodule bump is two
# lines in the diff and an arbitrary amount of new code in the binary, so its
# line count means nothing; `filter_job.py` never treats such a PR as small.
PRODUCT_CODE_PATHS = (
    "src/",
    "base/",
    "programs/",
    "rust/",
    "cmake/",
    "CMakeLists.txt",
    "PreLoad.cmake",
)


def get_product_changed_lines(info):
    """Lines changed (additions + deletions) under `PRODUCT_CODE_PATHS` in the PR,
    per GitHub's per-file `changes` counter from the paginated `pulls/{pr}/files`
    listing.

    A renamed file counts when either side of the rename is product code, so that
    moving a source file out of `src/` and editing it on the way counts as the
    product-code change it is, instead of as nothing.

    Raises on any failure: the caller decides whether a missing count is fatal."""
    selector = " or ".join(f'startswith("{path}")' for path in PRODUCT_CODE_PATHS)
    # One `select` per side of a rename; an entry matching both is still counted once.
    jq = (
        f'[.[] | select((.filename | {selector}) '
        f'or ((.previous_filename // "") | {selector})) | .changes] | add // 0'
    )
    out = GH.get_output_with_retries(
        f"gh api repos/{info.repo_name}/pulls/{info.pr_number}/files --paginate "
        f"--jq '{jq}'",
        verbose=True,
        strict=True,
    )
    # `--paginate` with `--jq` prints one line per page.
    return sum(int(line) for line in out.split())


def store_settings_declaration_changes(info, changed_files):
    """Record what the settings-history style check needs: what the change does to the history
    records and declarations (see `parse_declaration_changes`), or else why it could not be
    determined.

    Fail-close: the check refuses to pass without one of the two keys. Never raises - that
    would break the changed_files storage other merge-queue jobs depend on."""
    try:
        # In a merge-queue run PR_NUMBER is 0; the queue entry is built for exactly one PR,
        # so use its linked PR number (same as GH.get_changed_files).
        pr_number = info.pr_number
        if pr_number <= 0 and info.is_merge_queue_event:
            pr_number = info.linked_pr_number
        files = {
            path: fetch_patch_and_file(info.repo_name, pr_number, path)
            for path in SETTINGS_DECLARATION_FILES
            if path in changed_files
        }
        changes = parse_declaration_changes(files)
        info.store_kv_data("settings_declaration_changes", changes)
        print(f"Stored settings declaration changes: {changes}")
    except Exception as e:
        message = settings_history_fetch_error_message(e)
        print(f"WARNING: failed to compute settings declaration changes: {message}")
        info.store_kv_data("settings_history_fetch_error", message)


if __name__ == "__main__":
    info = Info()

    # store changed files
    # Fail-close for PR and merge-queue runs: the merge-queue flaky check
    # selects tests from this list, so an empty fallback would silently skip
    # it. Do not fail for master/release CI workflows.
    changed_files = (
        GH.get_changed_files(strict=bool(info.pr_number) or info.is_merge_queue_event)
        or []
    )
    info.store_kv_data("changed_files", changed_files)

    # For the settings-history style check (check_style.py): when a settings declaration file
    # changed in a PR or merge-queue run, record which history records and declarations the
    # change touches. On success only setting names and versions are stored (never the raw
    # diff) to keep the pipeline `data` output small and free of user-authored free text
    # (see the note further below about the GH Actions runner dropping outputs that match a
    # secret pattern). On failure the reason is stored instead, separately bounded; it can
    # carry a capped slice of the API output.
    if (info.pr_number or info.is_merge_queue_event) and any(
        path in changed_files for path in SETTINGS_DECLARATION_FILES
    ):
        store_settings_declaration_changes(info, changed_files)

    # hack to get build digest
    some_build_job = copy.deepcopy(JobConfigs.build_jobs[0])
    some_build_job.run_in_docker = ""
    some_build_job.provides = []
    digest = Digest().calc_job_digest(some_build_job, {}, {}).split("-")[0]
    info.store_kv_data("build_digest", digest)

    # store recent master commits (used by bugfix validation to find builds, and by perf tests).
    # Store unconditionally: synced PRs in the private repo run the same bugfix validation
    # jobs, and both this query and the build artifacts in `find_master_builds` use the
    # public upstream namespace regardless of the repo the workflow runs in.
    raw = Shell.get_output(
        "gh api 'repos/ClickHouse/ClickHouse/commits?sha=master&per_page=50' -q '.[].sha'",
        verbose=True,
    )
    master_commits = raw.splitlines()
    info.store_kv_data("master_commits", master_commits)

    if info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
        # Store the previous commits for perf tests. The raw listing above is
        # not usable here: it interleaves merged PRs' side-branch commits, so a
        # consumer walking it commit by commit can run out of entries before
        # reaching the previous actual master run. Walk the first-parent chain
        # from the commit under test instead - every entry is a master commit,
        # and starting at `info.sha` also drops the commits pushed after this
        # run was triggered.
        commits = get_master_first_parent_commits(info.sha, MASTER_TRACK_COMMITS + 1)

        # Drop the current commit itself so the performance test compares against
        # the previous commit on master (commit-to-commit). Otherwise the job picks
        # the current commit's own build as the baseline and compares it against
        # itself, so a red status could never point at the commit that introduced
        # a regression.
        if commits and commits[0] == info.sha:
            commits.pop(0)

        info.store_kv_data("master_track_commits_sha", commits)

    if info.pr_number > 0:
        # Store how many lines of product code the PR changes: `filter_job.py` skips
        # the stress tests, fuzzers and SQL suites on small PRs. On failure the key stays absent,
        # and the hook then runs those jobs rather than skipping them on a missing
        # count.
        try:
            info.store_kv_data("product_changed_lines", get_product_changed_lines(info))
        except Exception as e:
            print(f"Failed to count changed lines of product code: {e}")

    merge_base_commit_sha = ""
    if info.pr_number > 0:
        # store merge base between master and current branch
        try:
            # A stored merge base is a full commit id, or the key is absent.
            merge_base_commit_sha = GH.get_output_with_retries(
                f"gh api repos/ClickHouse/ClickHouse/compare/master...{info.sha} -q .merge_base_commit.sha",
                verbose=True,
                strict=True,
            ).strip()
            if not _is_commit_sha(merge_base_commit_sha):
                raise RuntimeError(
                    f"merge base is not a commit id: [{merge_base_commit_sha[:200]}]"
                )
            info.store_kv_data("merge_base_commit_sha", merge_base_commit_sha)

        except Exception as e:
            print(f"Failed to get merge base via the GitHub API: {e}")

    # store integration test diff to find: TODO: find changed test cases
    if info.pr_number:
        # store master side commits for perf tests comparison
        if os.getenv("DISABLE_CI_MERGE_COMMIT") == "1":
            # HEAD is the raw PR head in this mode, so HEAD^1 is another PR
            # commit. Walk master from the merge base resolved above instead.
            master_parent_commits = (
                get_master_first_parent_commits(merge_base_commit_sha, 100)
                if _is_commit_sha(merge_base_commit_sha)
                else []
            )
        else:
            # In normal PR CI, HEAD is GitHub's synthetic merge commit and
            # HEAD^1 is the exact master revision tested by the workflow.
            master_parent = Shell.get_output(
                "git rev-parse HEAD^1", verbose=True
            ).strip()
            master_parent_commits = []
            if master_parent:
                master_parent_commits = [
                    sha.strip()
                    for sha in Shell.get_output(
                        # 100 commits gives enough range to find 5-6 recent master coverage
                        # .info files even when coverage runs are sparse (only some master
                        # commits publish coverage). 30 was too few -- the 6th baseline could
                        # be 80+ commits back with a meaningfully different test set.
                        f"git rev-list --first-parent --max-count=100 {master_parent}",
                        verbose=True,
                    ).splitlines()
                    if sha.strip()
                ]

        if master_parent_commits:
            info.store_kv_data("master_track_commits_sha", master_parent_commits)
            print(
                f"Stored {len(master_parent_commits)} master commits for perf test comparison, "
                f"starting from {master_parent_commits[0]}"
            )
        else:
            print("WARNING: Could not find master commits for perf test comparison")

        # Record which integration test files changed so a downstream job can
        # find the changed test cases (TODO). Store only the file paths, never
        # the raw `git diff` output: that diff is user-authored free text and
        # ends up serialized into the initial `Config Workflow` job's `data`
        # output (see Runner.run). The GitHub Actions runner scans job outputs
        # with built-in secret patterns and silently drops the whole output on
        # a match (e.g. a test fixture containing `Authorization: Bearer ...`),
        # which makes every downstream job skip. A consumer can recompute the
        # diff for these paths on demand.
        changed_integration_tests = [
            file
            for file in changed_files
            if file.startswith("tests/integration/test") and file.endswith(".py")
        ]
        info.store_kv_data("changed_integration_tests", changed_integration_tests)

    elif info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
        # store commit sha of release branch base to find binary for performance comparison in the job script later
        release_branch_base_sha = CHVersion.get_release_version().githash
        print(f"Release branch base sha: {release_branch_base_sha}")
        assert release_branch_base_sha
        release_branch_base_sha_with_predecessors = [
            s.strip()
            for s in Shell.get_output(
                f"git rev-list --max-count=20 {release_branch_base_sha}", verbose=True
            ).splitlines()
        ]
        assert all(len(s) == 40 for s in release_branch_base_sha_with_predecessors)
        assert release_branch_base_sha_with_predecessors[0] == release_branch_base_sha
        info.store_kv_data(
            "release_branch_base_sha_with_predecessors",
            release_branch_base_sha_with_predecessors,
        )
        print(
            f"Found base commit sha for latest release branch with its predecessors: [{release_branch_base_sha_with_predecessors}]"
        )
