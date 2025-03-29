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

    version = CHVersion.get_current_version_as_dict()
    info.store_custom_data("version", version)
