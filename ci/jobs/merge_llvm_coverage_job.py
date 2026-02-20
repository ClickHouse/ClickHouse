import os
import subprocess
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory

    (
        current_commit_sha,
        merge_base_commit_sha,
        branch,
        base_branch,
        repo_name,
        pr_number,
    ) = Utils.get_git_info()

    os.environ["BRANCH"] = branch
    os.environ["CURRENT_COMMIT"] = current_commit_sha
    os.environ["BASE_BRANCH"] = base_branch
    os.environ["BASE_COMMIT"] = merge_base_commit_sha
    os.environ["REPO_NAME"] = repo_name
    os.environ["PR_NUMBER"] = str(pr_number)

    result = Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/scripts/merge_llvm_coverage.sh"],
    )

    Utils.compress_gz(
        f"{temp_dir}/llvm_coverage_html_report",
        f"{temp_dir}/llvm_coverage_html_report.tar.gz",
    )

    files_to_attach = []
    assets_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_report_dir = Path(temp_dir) / "llvm_coverage_html_report"
    if html_report_dir.exists():
        # Add index.html first as it's the entry point (root level only)
        index_file = html_report_dir / "index.html"
        if index_file.exists():
            files_to_attach.append(str(index_file))

        # Add all other files including index.html in subdirectories
        for file_path in html_report_dir.rglob("*"):
            if file_path.is_file() and file_path != index_file:
                assets_to_attach.append(str(file_path))

        Result.create_from(
            results=[result],
            files=files_to_attach,
            assets=assets_to_attach,
            info="LLVM Coverage Merge Job Completed",
        ).complete_job(disable_attached_files_sorting=True)
