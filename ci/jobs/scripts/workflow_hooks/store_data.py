import copy

from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika.digest import Digest
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.utils import Shell

if __name__ == "__main__":
    info = Info()

    # store changed files
    changed_files = (
        GH.get_changed_files(strict=info.pr_number) or []
    )  # do not fail for master/release CI workflow
    info.store_kv_data("changed_files", changed_files)

    # hack to get build digest
    some_build_job = copy.deepcopy(JobConfigs.build_jobs[0])
    some_build_job.run_in_docker = ""
    some_build_job.provides = []
    digest = Digest().calc_job_digest(some_build_job, {}, {}).split("-")[0]
    info.store_kv_data("build_digest", digest)

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

        info.store_kv_data("previous_commits_sha", commits)

    # Unshallow repo to retrieve required info from git.
    # If commit is a mere-commit - both parents need to be unshallowed. In PRs we might need commits from master to calculate CH version.
    Shell.check(
        f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin HEAD ||:",
        verbose=True,
        strict=True,
    )

    # store integration test diff to find: TODO: find changed test cases
    if info.pr_number:
        file_diff = {}
        for file in changed_files:
            if file.startswith("tests/integration/test") and file.endswith(".py"):
                file_diff[file] = Shell.get_output(
                    f"git diff $(git merge-base master HEAD)..HEAD -- {file}",
                    verbose=True,
                )
        info.store_kv_data("file_diff", file_diff)

    # store commit sha of release branch base to find binary for performance comparison in the job script later
    # if info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
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
    info.store_kv_data(
        "release_branch_base_sha_with_predecessors",
        release_branch_base_sha_with_predecessors,
    )
    print(
        f"Found base commit sha for latest release branch with its predecessors: [{release_branch_base_sha_with_predecessors}]"
    )
