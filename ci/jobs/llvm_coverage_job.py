import json
import os
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from ci.praktika._environment import _Environment
from ci.praktika.info import Info
from ci.praktika.mangle import _get_workflows
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
import ci.praktika.cidb as CIDB
from ci.praktika.settings import Settings
from ci.defs.defs import S3_REPORT_BUCKET_HTTP_ENDPOINT

WORKFLOW = _get_workflows(name=_Environment.get().WORKFLOW_NAME)[0]
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


def add_html_report_to_ci(folder_path, entry_point: str = "index.html"):
    files_to_attach = []
    assets_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_report_dir = Path(TEMP_DIR) / folder_path
    if html_report_dir.exists():
        # Add the entry-point file first so it is easily identifiable
        index_file = html_report_dir / entry_point
        if index_file.exists():
            files_to_attach.append(str(index_file))

        # Add all other files including index.html in subdirectories
        for file_path in html_report_dir.rglob("*"):
            if file_path.is_file() and file_path != index_file:
                assets_to_attach.append(str(file_path))
    return files_to_attach, assets_to_attach


def save_date_into_ci_db(
    date_str: str,
    pr_number: int,
    current_commit_sha: str,
    branch: str,
    merge_base_commit_sha: str,
    base_branch: str,
    b_line_cov: float,
    b_function_cov: float,
    b_branch_cov: float,
    c_line_cov: float,
    c_function_cov: float,
    c_branch_cov: float,
    delta: float,
    status: str,
    coverage_report_url: str = "",
    diff_coverage_report_url: str = "",
    uncovered_code_url: str = "",
):
    cidb = CIDB.CIDB(
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_USER).get_value(),
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_PASSWORD).get_value(),
    )
    is_ok, error = cidb.check()
    if not is_ok:
        raise RuntimeError(f"CI DB connection check failed: {error}")

    row = json.dumps(
        {
            "check_start_time": date_str,
            "pull_request_number": pr_number,
            "commit_sha": current_commit_sha,
            "base_commit_sha": merge_base_commit_sha,
            "branch": branch,
            "base_branch": base_branch,
            "status": status,
            "baseline_line_cov": b_line_cov,
            "baseline_func_cov": b_function_cov,
            "baseline_branch_cov": b_branch_cov,
            "current_line_cov": c_line_cov,
            "current_func_cov": c_function_cov,
            "current_branch_cov": c_branch_cov,
            "delta_line_cov": delta,
            "coverage_report_url": coverage_report_url,
            "diff_coverage_report_url": diff_coverage_report_url,
            "uncovered_code_url": uncovered_code_url,
        }
    )

    # Temporarily point insert_rows at our custom database/table.
    _orig_db, _orig_table = Settings.CI_DB_DB_NAME, Settings.CI_DB_TABLE_NAME
    try:
        Settings.CI_DB_DB_NAME = "coverage_ci"
        Settings.CI_DB_TABLE_NAME = "coverage_data"
        cidb.insert_rows([row])
    finally:
        Settings.CI_DB_DB_NAME, Settings.CI_DB_TABLE_NAME = _orig_db, _orig_table


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

    results = []

    results.append(
        Result.from_commands_run(
            name="Generate LLVM Coverage Report",
            command=["bash ci/jobs/scripts/merge_llvm_coverage.sh"],
        )
    )

    # Compress the diff HTML report for full coverage
    Utils.compress_gz(
        f"{TEMP_DIR}/llvm_coverage_html_report",
        f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
    )

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
            f"{TEMP_DIR}/current_llvm_coverage.info"
        )

        delta = c_line_cov - b_line_cov
        print(f"Baseline coverage : {b_line_cov:.2f}%")
        print(f"Current coverage  : {c_line_cov:.2f}%")
        print(f"Delta             : {delta:+.2f}%")

        if c_line_cov < b_line_cov:
            diff_res.set_failed()
            diff_res.set_comment(
                f"Lines: {b_line_cov:.2f}% → {c_line_cov:.2f}% (Δ {delta:+.2f}%)"
            )
            print("Coverage degraded.")
        else:
            print("Coverage did not degrade.")

        results.append(diff_res)

        # Generate report for changed blocks only
        print_res = Result.from_commands_run(
            name="Print uncovered changed code with context",
            command=["python3 ci/jobs/scripts/print_uncovered_code.py"],
            with_log=True,
            with_info=True,
            with_info_on_failure=True,
        )
        # print_res.set_status(diff_res.status)
        results.append(print_res)

        # Rename the diff report entry-point so it does not clash with the
        # full-coverage report's index.html when both are served from the same S3 prefix.
        _diff_index = Path(f"{TEMP_DIR}/llvm_coverage_diff_html_report/index.html")
        _diff_index_renamed = _diff_index.with_name("diff_index.html")
        if _diff_index.exists():
            _diff_index.rename(_diff_index_renamed)

        # Compress the diff HTML report
        Utils.compress_gz(
            f"{TEMP_DIR}/llvm_coverage_diff_html_report",
            f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz",
        )

        if not info.is_local_run:
            # Construct S3 artifact URLs from the known upload path structure:
            #   files/assets → https://<report_endpoint>/<s3_prefix>/<path_relative_to_TEMP_DIR>
            #   log files    → https://<report_endpoint>/<s3_prefix>/<normalize(result.name)>/<log_basename>
            _env = _Environment.get()
            _s3_base = (
                f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_env.get_s3_prefix()}"
            )
            _log_name = f"{Utils.normalize_string(print_res.name)}.log"

            save_date_into_ci_db(
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                pr_number,
                current_commit_sha,
                branch,
                merge_base_commit_sha,
                base_branch,
                b_line_cov,
                b_function_cov,
                b_branch_cov,
                c_line_cov,
                c_function_cov,
                c_branch_cov,
                delta,
                diff_res.status,
                coverage_report_url=f"{_s3_base}/llvm_coverage/index.html",
                diff_coverage_report_url=f"{_s3_base}/llvm_coverage/diff_index.html",
                uncovered_code_url=f"{_s3_base}/llvm_coverage/{Utils.normalize_string(print_res.name)}/{_log_name}",
            )
        else:
            print("Local run, skipping CI DB update with coverage results")
    else:
        print("On master branch, skipping diff coverage generation")

    files_to_attach = []
    assets_to_attach = []

    # Attach merged HTML report for full coverage
    files_to_attach.append(f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz")
    merged_files, merged_assets = add_html_report_to_ci("llvm_coverage_html_report")
    files_to_attach.extend(merged_files)
    assets_to_attach.extend(merged_assets)

    if not is_master_branch:
        # Attach merged HTML report for diff coverage
        files_to_attach.append(f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz")
        merged_files, merged_assets = add_html_report_to_ci(
            "llvm_coverage_diff_html_report", entry_point="diff_index.html"
        )
        files_to_attach.extend(merged_files)
        assets_to_attach.extend(merged_assets)

    Result.create_from(
        results=results,
        files=files_to_attach,
        assets=assets_to_attach,
        info="LLVM Coverage Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
