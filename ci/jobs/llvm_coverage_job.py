import os
import re
import shlex
import shutil
from datetime import datetime, timezone
from pathlib import Path
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from ci.praktika.gh import GH
from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.defs.defs import S3_REPORT_BUCKET_HTTP_ENDPOINT

CURRENT_DIR = Utils.cwd()
TEMP_DIR = f"{CURRENT_DIR}/ci/tmp/"


def get_lcov_summary_percentages(info_file_path: str) -> tuple[float, float, float]:
    # lcov --summary writes to stderr, so merge stderr into stdout with 2>&1
    output = Shell.get_output(
        " ".join(
            [
                "lcov",
                "--ignore-errors",
                "inconsistent,corrupt",
                "--branch-coverage",
                "--summary",
                shlex.quote(info_file_path),
                "2>&1",
            ]
        ),
        strict=True,
        verbose=True,
    )

    def extract_percent(metric: str) -> float:
        match = re.search(
            rf"^\s*{metric}\.*:\s*([0-9]+(?:\.[0-9]+)?)%", output, re.MULTILINE
        )
        if match:
            return float(match.group(1))
        if re.search(rf"^\s*{metric}\.*:\s*no data found", output, re.MULTILINE):
            raise ValueError(
                f"lcov summary contains no data for '{metric}'. "
                "Make sure you run lcov with --branch-coverage when you need branch stats."
            )
        raise ValueError(
            f"Failed to parse '{metric}' percentage from lcov output:\n{output}"
        )

    return (
        extract_percent("lines"),
        extract_percent("functions"),
        extract_percent("branches"),
    )


def collect_html_report_files(
    folder_path: str, entry_point: str = "index.html"
) -> tuple[list[str], list[str]]:
    """Return (files, assets) for an HTML report folder.

    The entry-point file goes into `files` (uploaded individually and linked),
    while every other file goes into `assets`.  Both lists must be attached to
    the *same* Result so that upload_result_files_to_s3 computes
    common_root = <folder>, keeping genhtml relative links intact on S3.
    """
    html_report_dir = Path(TEMP_DIR) / folder_path
    files: list[str] = []
    assets: list[str] = []
    if html_report_dir.exists():
        index_file = html_report_dir / entry_point
        if index_file.exists():
            files.append(str(index_file))
        for file_path in html_report_dir.rglob("*"):
            if file_path.is_file() and file_path != index_file:
                assets.append(str(file_path))
    return files, assets


def get_git_info() -> tuple[str, str, str, str, str, str]:
    # Get git info from Info singleton, if not present, get it from shell commands
    # merge_base_commit_sha, branch, base_branch, repo_name, pr_number
    info = Info()

    current_commit_sha = info.sha
    if current_commit_sha is None:
        current_commit_sha = Shell.get_output(
            "git rev-parse HEAD", verbose=True
        ).strip()

    merge_base_commit_sha = info.get_kv_data("merge_base_commit_sha")
    if merge_base_commit_sha is None:
        # Use gh api to get the merge base commit between master and HEAD
        merge_base_commit_sha = Shell.get_output(
            f"gh api repos/ClickHouse/ClickHouse/compare/master...{current_commit_sha} -q .merge_base_commit.sha",
            verbose=True,
        ).strip()

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
    pr_number = (
        info.pr_number
        if info.pr_number > 0
        else Shell.get_output(
            "gh pr view --json number -q .number", verbose=True
        ).strip()
    )
    return (
        current_commit_sha,
        merge_base_commit_sha,
        branch,
        base_branch,
        repo_name,
        pr_number,
    )


if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = CURRENT_DIR

    info = Info()

    (
        current_commit_sha,
        merge_base_commit_sha,
        branch,
        base_branch,
        repo_name,
        pr_number,
    ) = get_git_info()

    prev_30_commits = []
    if int(pr_number) > 0:
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

    results = []

    gen_report_res = Result.from_commands_run(
        name="Generate LLVM Coverage Report",
        command=["bash ci/jobs/scripts/merge_llvm_coverage.sh"],
    )
    # Compress and attach the full HTML report archive + files to the generate result.
    # Keeping files/assets inside the same sub-Result ensures upload_result_files_to_s3
    # computes common_root = llvm_coverage_html_report/, so relative links stay intact.
    Utils.compress_gz(
        f"{TEMP_DIR}/llvm_coverage_html_report",
        f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
    )
    gen_report_res.files.append(f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz")
    _html_files, _html_assets = collect_html_report_files("llvm_coverage_html_report")
    gen_report_res.files.extend(_html_files)
    gen_report_res.assets.extend(_html_assets)
    results.append(gen_report_res)

    if not is_master_branch:
        diff_res = Result.from_commands_run(
            name="Generate LLVM Coverage Diff Report",
            command=["bash ci/jobs/scripts/generate_diff_coverage_report.sh"],
        )

        # Baseline coverage percentages for the current branch (from the merged report)
        b_line_cov, b_function_cov, b_branch_cov = get_lcov_summary_percentages(
            f"{TEMP_DIR}/base_llvm_coverage.info"
        )

        # Current coverage percentages for the current branch
        c_line_cov, c_function_cov, c_branch_cov = get_lcov_summary_percentages(
            f"{TEMP_DIR}/llvm_coverage.info"
        )

        delta = c_line_cov - b_line_cov
        print(f"Baseline coverage : {b_line_cov:.2f}%")
        print(f"Current coverage  : {c_line_cov:.2f}%")
        print(f"Delta             : {delta:+.2f}%")

        if c_line_cov < b_line_cov:
            diff_res.set_failed()
            diff_res.set_comment(
                f"Coverage in main branch: {b_line_cov:.2f}%, coverage in PR: {c_line_cov:.2f}%. Coverage degraded by {delta:.2f} percentage points."
            )
            print("Coverage degraded.")
        else:
            print("Coverage did not degrade.")

        # Compress and attach the diff HTML report archive + files to the diff result.
        Utils.compress_gz(
            f"{TEMP_DIR}/llvm_coverage_diff_html_report",
            f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz",
        )
        diff_res.files.append(f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz")
        # Copy index.html → index_diff.html so the diff entry-point has a unique
        # name in S3 links. The original index.html is kept as an asset so that
        # relative links inside the report continue to work.
        _diff_index = Path(TEMP_DIR) / "llvm_coverage_diff_html_report" / "index.html"
        _diff_index_copy = _diff_index.parent / "index_diff.html"
        if _diff_index.exists():
            shutil.copy2(_diff_index, _diff_index_copy)
        _diff_files, _diff_assets = collect_html_report_files(
            "llvm_coverage_diff_html_report", entry_point="index_diff.html"
        )
        diff_res.files.extend(_diff_files)
        diff_res.assets.extend(_diff_assets)
        results.append(diff_res)

        # Generate report for changed blocks only
        _print_log = f"{TEMP_DIR}{Utils.normalize_string('Print Uncovered Code')}.log"
        Shell.run(
            f"python3 ci/jobs/scripts/print_uncovered_code.py 2>&1 | tee {_print_log}",
            verbose=True,
        )
        print_res = Result.from_fs("Print Uncovered Code")
        print_res.files.append(_print_log)
        results.append(print_res)

        if not info.is_local_run:
            # Construct S3 artifact URLs from the known upload path structure:
            #   HTML files/assets → https://<endpoint>/<s3_prefix>/<normalize(job)>/<normalize(sub_result)>/<rel_path>
            #   log files         → https://<endpoint>/<s3_prefix>/<normalize(job)>/<normalize(result)>/<log_basename>
            _s3_prefix = f"PRs/{pr_number}/{current_commit_sha}" if int(pr_number) > 0 else f"REFs/{branch}/{current_commit_sha}"
            _s3_base = f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_s3_prefix}"
            _log_name = f"{Utils.normalize_string(print_res.name)}.log"
            uncovered_code_url = f"{_s3_base}/llvm_coverage/{Utils.normalize_string(print_res.name)}/{_log_name}"

            CIDBCluster().insert_json(
                table="coverage_ci.coverage_data",
                json_str={
                    "check_start_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "pull_request_number": pr_number,
                    "commit_sha": current_commit_sha,
                    "base_commit_sha": merge_base_commit_sha,
                    "branch": branch,
                    "base_branch": base_branch,
                    "status": diff_res.status,
                    "baseline_line_cov": b_line_cov,
                    "baseline_func_cov": b_function_cov,
                    "baseline_branch_cov": b_branch_cov,
                    "current_line_cov": c_line_cov,
                    "current_func_cov": c_function_cov,
                    "current_branch_cov": c_branch_cov,
                    "delta_line_cov": delta,
                    "coverage_report_url": f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html",
                    "diff_coverage_report_url": f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index.html",
                    "uncovered_code_url": uncovered_code_url,
                },
            )

            _diff_url = f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            _pr_changed_lines_info = print_res.ext.get("comment", "")
            _pr_changed_lines_row = (
                f"\n**PR changed lines:** {_pr_changed_lines_info}"
                if _pr_changed_lines_info
                else ""
            )
            GH.post_fresh_comment(
                tag="llvm-coverage",
                body=(
                    f"## LLVM Coverage Report\n"
                    f"| Metric | Baseline | Current | Δ |\n"
                    f"|--------|----------|---------|---|\n"
                    f"| Lines | {b_line_cov:.2f}% | {c_line_cov:.2f}% | {c_line_cov - b_line_cov:+.2f}% |\n"
                    f"| Functions | {b_function_cov:.2f}% | {c_function_cov:.2f}% | {c_function_cov - b_function_cov:+.2f}% |\n"
                    f"| Branches | {b_branch_cov:.2f}% | {c_branch_cov:.2f}% | {c_branch_cov - b_branch_cov:+.2f}% |\n"
                    f"{_pr_changed_lines_row}"
                    f"\n[Diff coverage report]({_diff_url})"
                    f"\n[Uncovered code]({uncovered_code_url})"
                ),
            )
        else:
            print("Local run, skipping CI DB update with coverage results")
    else:
        print("On master branch, skipping diff coverage generation")

    # Add direct S3 links to both HTML reports in the main job result.
    # HTML files are uploaded within the corresponding generate sub-result;
    # the URL is deterministic: llvm_coverage/<normalize(sub_result_name)>/<filename>.
    report_links = []
    if not info.is_local_run:
        _s3_prefix = f"PRs/{pr_number}/{current_commit_sha}" if int(pr_number) > 0 else f"REFs/{branch}/{current_commit_sha}"
        _s3_base = f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_s3_prefix}"
        report_links.append(
            f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html"
        )
        if not is_master_branch:
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            )

    archives = [f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz"]
    if not is_master_branch:
        archives.append(f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz")

    Result.create_from(
        results=results,
        files=archives,
        links=report_links,
        info="LLVM Coverage Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
