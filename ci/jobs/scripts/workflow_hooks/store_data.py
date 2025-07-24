import copy

from ci.defs.job_configs import JobConfigs
from ci.praktika.digest import Digest
from ci.praktika.info import Info
from ci.praktika.utils import Shell
from ci.jobs.scripts.clickhouse_version import CHVersion

if __name__ == "__main__":
    info = Info()

    # store changed files
    if info.pr_number > 0:
        exit_code, changed_files_str, err = Shell.get_res_stdout_stderr(
            f"gh pr view {info.pr_number} --repo {info.repo_name} --json files --jq '.files[].path'",
        )
        assert exit_code == 0, "Failed to retrive changed files list"
    else:
        exit_code, changed_files_str, err = Shell.get_res_stdout_stderr(
            f"gh api repos/{info.repo_name}/commits/{info.sha} | jq -r '.files[].filename'",
        )
        # not fail on master or release branches

    if exit_code == 0:
        changed_files = changed_files_str.split("\n") if changed_files_str else []
        info.store_custom_data("changed_files", changed_files)

    # hack to get build digest
    some_build_job = copy.deepcopy(JobConfigs.build_jobs[0])
    some_build_job.run_in_docker = ""
    some_build_job.provides = []
    digest = Digest().calc_job_digest(some_build_job, {}, {}).split("-")[0]
    info.store_custom_data("build_digest", digest)

    if info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
        # store previous commits for perf tests
        raw = Shell.get_output(
            f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={info.git_branch}&per_page=30' -q '.[].sha' | head -n30",
            verbose=True,
        )
        commits = raw.splitlines()

        for sha in commits:
            if sha == info.sha:
                break
            commits.pop(0)

        info.store_custom_data("previous_commits_sha", commits)

    # store commit sha of release branch base to find binary for performance comparison in the job script later
    # if info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
    Shell.check(
        f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin {info.git_branch} ||:"
    )
    release_branch_base_sha = CHVersion.get_release_version_as_dict().get("githash")
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
    info.store_custom_data(
        "release_branch_base_sha_with_predecessors",
        release_branch_base_sha_with_predecessors,
    )
    print(
        f"Found base commit sha for latest release branch with its predecessors: [{release_branch_base_sha_with_predecessors}]"
    )
