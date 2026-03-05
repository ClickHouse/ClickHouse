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

        while commits and commits[0] != info.sha:
            commits.pop(0)

        info.store_kv_data("master_track_commits_sha", commits)

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
                    f"git rev-list --first-parent --max-count=30 {master_parent}", verbose=True
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

        file_diff = {}
        for file in changed_files:
            if file.startswith("tests/integration/test") and file.endswith(".py"):
                file_diff[file] = Shell.get_output(
                    f"git diff $(git merge-base master HEAD)..HEAD -- {file}",
                    verbose=True,
                )
        info.store_kv_data("file_diff", file_diff)

    elif info.git_branch == "master" and info.repo_name == "ClickHouse/ClickHouse":
        # store commit sha of release branch base to find binary for performance comparison in the job script later
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
