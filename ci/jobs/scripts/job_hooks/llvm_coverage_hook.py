import json
import traceback
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.gh import GH
from ci.praktika.info import Info


def check():
    info = Info()

    if info.pr_number <= 0:
        print("Not a PR run, skipping coverage comment")
        return

    comment_file = Path("./ci/tmp/coverage_comment.json")
    if not comment_file.exists():
        print(f"Coverage comment data not found at {comment_file}, skipping")
        return

    try:
        with open(comment_file) as f:
            d = json.load(f)

        b_line_cov = d["b_line_cov"]
        c_line_cov = d["c_line_cov"]
        b_function_cov = d["b_function_cov"]
        c_function_cov = d["c_function_cov"]
        b_branch_cov = d["b_branch_cov"]
        c_branch_cov = d["c_branch_cov"]
        pr_changed_lines_info = d.get("pr_changed_lines_info", "")
        diff_url = d["diff_url"]
        uncovered_code_url = d["uncovered_code_url"]

        pr_changed_lines_row = (
            f"\n**PR changed lines:** {pr_changed_lines_info}"
            if pr_changed_lines_info
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
                f"{pr_changed_lines_row}"
                f"\n[Diff coverage report]({diff_url})"
                f"\n[Uncovered code]({uncovered_code_url})"
            ),
        )

        CIDBCluster().insert_json(
            table="coverage_ci.coverage_data",
            json_str={
                "check_start_time": d["check_start_time"],
                "pull_request_number": d["pull_request_number"],
                "commit_sha": d["commit_sha"],
                "base_commit_sha": d["base_commit_sha"],
                "branch": d["branch"],
                "base_branch": d["base_branch"],
                "status": d["status"],
                "baseline_line_cov": b_line_cov,
                "baseline_func_cov": b_function_cov,
                "baseline_branch_cov": b_branch_cov,
                "current_line_cov": c_line_cov,
                "current_func_cov": c_function_cov,
                "current_branch_cov": c_branch_cov,
                "delta_line_cov": d["delta_line_cov"],
                "coverage_report_url": d["coverage_report_url"],
                "diff_coverage_report_url": d["diff_coverage_report_url"],
                "uncovered_code_url": uncovered_code_url,
            },
        )
    except Exception:
        print("ERROR: Failed to post coverage comment or insert into CIDB")
        traceback.print_exc()


if __name__ == "__main__":
    check()
