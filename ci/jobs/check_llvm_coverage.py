import os
from pathlib import Path
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
import subprocess, json

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory

    info = Info()
    merge_base_commit_sha = info.get_kv_data("merge_base_commit_sha")
    if merge_base_commit_sha is None:
        merge_base_commit_sha = Shell.get_output(
            "git merge-base origin/master HEAD", verbose=True
        ).strip()
    os.environ["BASE_COMMIT"] = merge_base_commit_sha

    prev_30_commits = info.get_kv_data("master_commits_before_merge_base")
    if prev_30_commits is None:
        # Get merge base commit and next 30 newer commits in master (oldest to newest)
        master_commits = Shell.get_output(
            "git rev-list origin/master --max-count=500", verbose=True
        ).splitlines()
        if merge_base_commit_sha in master_commits:
            idx = master_commits.index(merge_base_commit_sha)
            # List: merge base + next 30 newer commits
            prev_30_commits = master_commits[idx:idx+31]
    os.environ["PREV_30_COMMITS"] = ",".join(prev_30_commits or [])

    current_commit_sha = info.get_kv_data("current_commit_sha")
    if current_commit_sha is None:
        current_commit_sha = Shell.get_output("git rev-parse HEAD", verbose=True).strip()
    os.environ["CURRENT_COMMIT"] = current_commit_sha

    result = Result.from_commands_run(
        name="LLVM Coverage Check",
        command=["bash ci/jobs/scripts/diff_coverage.sh"],
    )

    Utils.compress_gz(
        f"{temp_dir}/llvm_coverage_diff_html_report",
        f"{temp_dir}/llvm_coverage_diff_html_report.tar.gz",
    )

    files_to_attach = []
    assets_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_diff_report_dir = Path(temp_dir) / "llvm_coverage_diff_html_report"
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
