import os
from pathlib import Path
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
import subprocess, json

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    info = Info()
    prev_10_commits = info.get_kv_data("master_commits_after_merge_base")
    if prev_10_commits is None:
        # Get merge base commit between master and current HEAD
        base_commit_sha = Shell.get_output(
            "git merge-base master HEAD", verbose=True
        ).strip()
        info.store_kv_data("merge_base_sha", base_commit_sha)

        # Get 10 previous commits from master after the base commit
        # List master commits, find base commit, take next 10
        master_commits = Shell.get_output(
            "git rev-list master --max-count=100", verbose=True
        ).splitlines()
        if base_commit_sha in master_commits:
            idx = master_commits.index(base_commit_sha)
            prev_10_commits = master_commits[idx : idx + 10]
        else:
            prev_10_commits = master_commits[:10]
        info.store_kv_data("master_commits_after_merge_base", prev_10_commits)

    os.environ["PREV_10_COMMITS"] = ",".join(prev_10_commits or [])

    # Pass workspace path to the shell script via environment variable
    # os.environ["WORKSPACE_PATH"] = current_directory

    result = Result.from_commands_run(
        name="LLVM Coverage Check",
        command=["bash ci/jobs/scripts/diff_coverage.sh"],
    )

    Utils.compress_gz(
        f"{temp_dir}/diff-html",
        f"{temp_dir}/diff-html.tar.gz",
    )

    files_to_attach = []
    assets_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_diff_report_dir = Path(temp_dir) / "diff-html"
    if html_diff_report_dir.exists():
        # Add index.html first as it's the entry point (root level only)
        index_file = html_diff_report_dir / "index.html"
        if index_file.exists():
            files_to_attach.append(str(index_file))

        # Add all other files including index.html in subdirectories
        for file_path in html_diff_report_dir.rglob("*"):
            if file_path.is_file() and file_path != index_file:
                assets_to_attach.append(str(file_path))

    Result.create_from(
        results=[result],
        files=files_to_attach,
        assets=assets_to_attach,
        info="LLVM Coverage Check Completed",
    ).complete_job(disable_attached_files_sorting=True)
