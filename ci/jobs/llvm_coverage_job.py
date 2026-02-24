import os
from pathlib import Path
from ci.praktika.info import Info

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

def add_html_report_to_ci(folder_path):
    files_to_attach = []
    assets_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_report_dir = Path(temp_dir) / folder_path
    if html_report_dir.exists():
        # Add index.html first as it's the entry point (root level only)
        index_file = html_report_dir / "index.html"
        if index_file.exists():
            files_to_attach.append(str(index_file))

        # Add all other files including index.html in subdirectories
        for file_path in html_report_dir.rglob("*"):
            if file_path.is_file() and file_path != index_file:
                assets_to_attach.append(str(file_path))
    return files_to_attach, assets_to_attach

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory

    info = Info()

    (
        current_commit_sha,
        merge_base_commit_sha,
        branch,
        base_branch,
        repo_name,
        pr_number,
    ) = Utils.get_git_info()

    prev_30_commits = info.get_kv_data("master_commits_before_merge_base")
    if prev_30_commits is None:
        # Get 30 commits starting from merge base commit and walking backwards.
        raw = Shell.get_output(
            f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={merge_base_commit_sha}&per_page=30' -q '.[].sha'",
            verbose=True,
        )
        prev_30_commits = raw.splitlines()

    os.environ["BRANCH"] = branch
    os.environ["CURRENT_COMMIT"] = current_commit_sha
    os.environ["BASE_BRANCH"] = base_branch
    os.environ["BASE_COMMIT"] = merge_base_commit_sha
    os.environ["REPO_NAME"] = repo_name
    os.environ["PR_NUMBER"] = str(pr_number)
    os.environ["PREV_30_COMMITS"] = ",".join(prev_30_commits or [])
    
    is_master_branch = branch == "master"

    results=[]

    results.append(Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/scripts/merge_llvm_coverage.sh"],
    ))
    Utils.compress_gz(
        f"{temp_dir}/llvm_coverage_html_report",
        f"{temp_dir}/llvm_coverage_html_report.tar.gz",
    )

    if not is_master_branch:
        results.append(
            Result.from_commands_run(
                name="LLVM Coverage Check",
                command=["bash ci/jobs/scripts/diff_coverage.sh"],
        ))

        # Generate report for changed blocks only
        results.append(
            Result.from_commands_run(
                name="Print uncovered changed code with context",
                command=["python3 ci/jobs/scripts/print_uncovered_code.py"],
                with_log=True,
                with_info=True,
                with_info_on_failure=True,
            )
        )
        Utils.compress_gz(
            f"{temp_dir}/llvm_coverage_diff_html_report",
            f"{temp_dir}/llvm_coverage_diff_html_report.tar.gz",
        )
    else:
        print("On master branch, skipping diff coverage generation")

    files_to_attach = []
    assets_to_attach = []

    # Attach merged HTML report for full coverage
    files_to_attach.append(f"{temp_dir}/llvm_coverage_html_report.tar.gz")
    merged_files, merged_assets = add_html_report_to_ci("llvm_coverage_html_report")
    files_to_attach.extend(merged_files)
    assets_to_attach.extend(merged_assets)

    if not is_master_branch:
        # Attach merged HTML report for diff coverage
        files_to_attach.append(f"{temp_dir}/llvm_coverage_diff_html_report.tar.gz")
        merged_files, merged_assets = add_html_report_to_ci("llvm_coverage_diff_html_report")
        files_to_attach.extend(merged_files)
        assets_to_attach.extend(merged_assets)

    Result.create_from(
        results=results,
        files=files_to_attach,
        assets=assets_to_attach,
        info="LLVM Coverage Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
