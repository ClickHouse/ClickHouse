import copy
import re

from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika.digest import Digest
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.utils import Shell

_SETTINGS_HISTORY_ENTRY_RE = re.compile(r'^\s*\{\s*"([A-Za-z0-9_]+)"')
_SETTINGS_HISTORY_BLOCK_RE = re.compile(r'addSettingsChanges\(\s*(\w+)\s*,\s*"[\d.]+"')
_SETTINGS_HISTORY_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
_SETTINGS_HISTORY_NAMESPACES = {
    "settings_changes_history": "Session",
    "merge_tree_settings_changes_history": "MergeTree",
}


def _settings_history_entry_signature(entry_body):
    """The entry with its trailing reason string stripped, so a value change is distinguished
    from a reason-only edit: `{"s", false, true, "..."}` differs but `{"s", false, false, "a"}`
    and `{"s", false, false, "b"}` share a signature."""
    return re.sub(r',\s*"(?:[^"\\]|\\.)*"\s*\},?\s*$', "", entry_body).strip()


def parse_settings_history_changes(patch, file_lines):
    """Given the unified diff of src/Core/SettingsChangesHistory.cpp and the lines of the file
    at HEAD, return a list of {"namespace", "name"} for settings whose recorded value changed or
    that were newly added (including an in-place value edit of an existing entry). Reason-only
    edits (an added entry whose value-signature was also removed) are ignored. The namespace
    comes from the block that physically contains each added line (new-file line number), not
    from global name presence - names can exist in both histories.

    Whether such a change must sit under the CURRENT version block is decided by the caller
    (check_settings_changes_history), which enforces the rule as soon as any other C++ source
    file changed. A change that edits only this file - fixing what a past release recorded -
    is a historical correction, not a default change made now, so it is allowed there."""
    added = []  # (new_line_number, name, signature)
    removed_signatures = set()
    new_lineno = None
    for line in patch.splitlines():
        hunk = _SETTINGS_HISTORY_HUNK_RE.match(line)
        if hunk:
            new_lineno = int(hunk.group(1))
            continue
        if new_lineno is None:
            continue
        if line.startswith("+") and not line.startswith("+++"):
            m = _SETTINGS_HISTORY_ENTRY_RE.match(line[1:])
            if m:
                added.append(
                    (new_lineno, m.group(1), _settings_history_entry_signature(line[1:]))
                )
            new_lineno += 1
        elif line.startswith("-") and not line.startswith("---"):
            m = _SETTINGS_HISTORY_ENTRY_RE.match(line[1:])
            if m:
                removed_signatures.add(_settings_history_entry_signature(line[1:]))
            # A removed line does not advance the new-file line counter.
        else:
            new_lineno += 1

    result = []
    seen = set()
    for lineno, name, signature in added:
        if signature in removed_signatures:
            continue  # reason-only edit of an existing entry
        namespace = None
        for i in range(min(lineno, len(file_lines)) - 1, -1, -1):
            mb = _SETTINGS_HISTORY_BLOCK_RE.search(file_lines[i])
            if mb:
                namespace = _SETTINGS_HISTORY_NAMESPACES.get(mb.group(1))
                break
        if namespace and (namespace, name) not in seen:
            seen.add((namespace, name))
            result.append({"namespace": namespace, "name": name})
    return result


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

    # For the settings-history style check (check_style.py): when
    # src/Core/SettingsChangesHistory.cpp changed in a PR or merge-queue run, record the
    # names of the setting entries this change ADDS so the style check can verify each is
    # recorded under the current version block. Only the setting names are stored (never the
    # raw diff) to keep the pipeline `data` output small and free of user-authored free text
    # (see the note further below about the GH Actions runner dropping outputs that match a
    # secret pattern). Best-effort: a failure here must not break the hook that stores
    # changed_files; the style check simply skips when nothing is stored.
    settings_history_file = "src/Core/SettingsChangesHistory.cpp"
    if (
        info.pr_number or info.is_merge_queue_event
    ) and settings_history_file in changed_files:
        # Fail-close: when this file changed, the settings-history style check MUST be able to
        # validate it. If the diff cannot be fetched, record the error so the style check fails
        # rather than silently passing. Do not raise here: that would break the changed_files
        # storage other merge-queue jobs depend on; the error is surfaced by the style check.
        try:
            # CI containers have no .git history, so fetch the file's patch via the GitHub
            # API rather than `git diff`. In a merge-queue run PR_NUMBER is 0; the queue
            # entry is built for exactly one PR, so use its linked PR number (same as
            # GH.get_changed_files).
            pr_number = info.pr_number
            if pr_number <= 0 and info.is_merge_queue_event:
                pr_number = info.linked_pr_number
            if pr_number <= 0:
                raise RuntimeError(
                    "could not resolve the PR number for the settings-history diff"
                )
            # `.patch` is the unified diff for just this file (hunks only, no file header).
            patch = Shell.get_output(
                f"gh api repos/{info.repo_name}/pulls/{pr_number}/files --paginate "
                f"--jq '.[] | select(.filename == \"{settings_history_file}\") | .patch'",
                verbose=True,
            )
            if patch.strip() in ("", "null"):
                # The file is in changed_files but no usable patch came back. GitHub omits the
                # per-file patch for very large diffs; the `.patch` field is then null, which
                # `jq -r` prints as the literal string "null". We cannot determine the changed
                # settings, so fail closed instead of assuming there is nothing to check.
                raise RuntimeError(
                    f"no patch returned for changed file {settings_history_file} "
                    "(GitHub omits the patch for very large diffs)"
                )
            with open(settings_history_file, "r", encoding="utf-8", errors="ignore") as f:
                file_lines = f.read().splitlines()
            changed_settings = parse_settings_history_changes(patch, file_lines)
            info.store_kv_data("settings_history_changed_settings", changed_settings)
            print(f"Stored settings-history changed settings: {changed_settings}")
        except Exception as e:
            print(f"WARNING: failed to compute settings-history changed settings: {e}")

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
        # store previous commits for perf tests
        commits = list(master_commits)

        # Drop commits newer than the one under test (they may have been pushed
        # after this run was triggered) so that commits[0] is the current commit.
        while commits and commits[0] != info.sha:
            commits.pop(0)

        # Drop the current commit itself so the performance test compares against
        # the previous commit on master (commit-to-commit). Otherwise the job picks
        # the current commit's own build as the baseline and compares it against
        # itself, so a red status could never point at the commit that introduced
        # a regression.
        if commits and commits[0] == info.sha:
            commits.pop(0)

        info.store_kv_data("master_track_commits_sha", commits)

    if info.pr_number > 0:
        # store merge base between master and current branch
        try:
            # Get the merge base commit using git
            merge_base_commit_sha = Shell.get_output(
                f"gh api repos/ClickHouse/ClickHouse/compare/master...{info.sha} -q .merge_base_commit.sha",
                verbose=True,
            ).strip()
            info.store_kv_data("merge_base_commit_sha", merge_base_commit_sha)

        except Exception as e:
            print(f"Failed to get merge base via git: {e}")

    # store integration test diff to find: TODO: find changed test cases
    if info.pr_number:
        # store master side commits for perf tests comparison
        # In PR CI, HEAD is a merge commit; HEAD^1 is the master parent (first parent)
        master_parent = Shell.get_output(
            "git rev-parse HEAD^1", verbose=True
        ).strip()
        if master_parent:
            master_parent_commits = [
                s.strip()
                for s in Shell.get_output(
                    # 100 commits gives enough range to find 5-6 recent master coverage
                # .info files even when coverage runs are sparse (only some master
                # commits publish coverage). 30 was too few — the 6th baseline could
                # be 80+ commits back with a meaningfully different test set.
                f"git rev-list --first-parent --max-count=100 {master_parent}", verbose=True
                ).splitlines()
                if s.strip()
            ]
            if master_parent_commits:
                info.store_kv_data("master_track_commits_sha", master_parent_commits)
                print(
                    f"Stored {len(master_parent_commits)} master parent commits for perf test comparison, starting from {master_parent}"
                )
        else:
            print(
                "WARNING: Could not find master parent commit (HEAD^1), skipping perf test commit storage"
            )

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
