from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika.info import Info
from ci.praktika.utils import Shell

if __name__ == "__main__":
    info = Info()

    # store changed files
    if info.pr_number > 0:
        changed_files_str = Shell.get_output(
            f"gh pr view {info.pr_number} --repo {info.repo_name} --json files --jq '.files[].path'",
            strict=True,
        )
    else:
        changed_files_str = Shell.get_output(
            f"gh api repos/{info.repo_name}/commits/{info.sha} | jq -r '.files[].filename'",
        )
    if changed_files_str:
        changed_files = changed_files_str.split("\n")
        info.store_custom_data("changed_files", changed_files)

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
