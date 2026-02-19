import os
import json
from pathlib import Path
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from praktika._environment import _Environment

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory

    info = Info()

    current_commit_sha = info.get_kv_data("current_commit_sha")
    if current_commit_sha is None:
        current_commit_sha = Shell.get_output(
            "git rev-parse HEAD", verbose=True
        ).strip()
    os.environ["CURRENT_COMMIT"] = current_commit_sha

    merge_base_commit_sha = info.get_kv_data("merge_base_commit_sha")
    if merge_base_commit_sha is None:
        # Use gh api to get the merge base commit between master and HEAD
        merge_base_commit_sha = Shell.get_output(
            f"gh api repos/ClickHouse/ClickHouse/compare/master...{current_commit_sha} -q .merge_base_commit.sha",
            verbose=True,
        ).strip()
        print(f"Merge base commit sha: {merge_base_commit_sha}")
    os.environ["BASE_COMMIT"] = merge_base_commit_sha

    prev_30_commits = info.get_kv_data("master_commits_before_merge_base")
    if prev_30_commits is None:
        # Get 30 commits starting from merge base commit and walking backwards.
        raw = Shell.get_output(
            f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={merge_base_commit_sha}&per_page=30' -q '.[].sha'",
            verbose=True,
        )
        prev_30_commits = raw.splitlines()
    os.environ["PREV_30_COMMITS"] = ",".join(prev_30_commits or [])

    branch = (
        info.git_branch
        or Shell.get_output("git branch --show-current", verbose=True).strip()
    )
    base_branch = info.base_branch or Shell.get_output(
        "gh pr view --json baseRefName --template '{{.baseRefName}}'", verbose=True
    ).strip().replace("origin/", "")
    repo_name = (
        info.repo_name
        or Shell.get_output(
            "basename -s .git `git config --get remote.origin.url`", verbose=True
        ).strip()
    )
    pr_number = Shell.get_output(
        "gh pr view --json number -q .number", verbose=True
    ).strip()
    os.environ["BRANCH"] = branch
    os.environ["BASE_BRANCH"] = base_branch
    os.environ["REPO_NAME"] = repo_name
    os.environ["PR_NUMBER"] = str(pr_number)

    results = []
    results.append(
        Result.from_commands_run(
            name="LLVM Coverage Check",
            command=["bash ci/jobs/scripts/diff_coverage.sh"],
        )
    )

    # Generate report for changed blocks only
    results.append(
        Result.from_commands_run(
            name="Print uncovered changed code with context",
            command=["python3 ci/jobs/scripts/print_uncovered_code.py"],
            with_log=True,
            with_info=True,
            with_info_on_failure=True
        )
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
        results=results, files=files_to_attach, assets=assets_to_attach
    ).complete_job(disable_attached_files_sorting=True)
