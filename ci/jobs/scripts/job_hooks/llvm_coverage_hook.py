import json
import traceback
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.gh import GH
from ci.praktika.info import Info


def check():
    info = Info()

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
        b_line_hit = d.get("b_line_hit", 0)
        b_line_total = d.get("b_line_total", 0)
        c_line_hit = d.get("c_line_hit", 0)
        c_line_total = d.get("c_line_total", 0)
        b_func_hit = d.get("b_func_hit", 0)
        b_func_total = d.get("b_func_total", 0)
        c_func_hit = d.get("c_func_hit", 0)
        c_func_total = d.get("c_func_total", 0)
        b_branch_hit = d.get("b_branch_hit", 0)
        b_branch_total = d.get("b_branch_total", 0)
        c_branch_hit = d.get("c_branch_hit", 0)
        c_branch_total = d.get("c_branch_total", 0)
        pr_changed_lines_info = d.get("pr_changed_lines_info", "")
        diff_url = d.get("diff_url", "")
        uncovered_code_url = d.get("uncovered_code_url", "")

        if info.pr_number > 0:
            body = (
                f"### LLVM Coverage Report\n\n"
                f"| Metric | Baseline | Current | Δ |\n"
                f"|--------|----------|---------|---|\n"
                f"| Lines | {b_line_cov:.2f}% | {c_line_cov:.2f}% | {c_line_cov - b_line_cov:+.2f}% |\n"
                f"| Functions | {b_function_cov:.2f}% | {c_function_cov:.2f}% | {c_function_cov - b_function_cov:+.2f}% |\n"
                f"| Branches | {b_branch_cov:.2f}% | {c_branch_cov:.2f}% | {c_branch_cov - b_branch_cov:+.2f}% |\n"
            )
            if pr_changed_lines_info:
                changed_line = f"\n**Changed lines:** {pr_changed_lines_info}"
                if uncovered_code_url:
                    changed_line += f" · [Uncovered code]({uncovered_code_url})"
                body += changed_line + "\n"
            links = []
            if coverage_report_url := d.get("coverage_report_url", ""):
                links.append(f"[Full report]({coverage_report_url})")
            if diff_url:
                links.append(f"[Diff report]({diff_url})")
            if not pr_changed_lines_info and uncovered_code_url:
                links.append(f"[Uncovered code]({uncovered_code_url})")
            if links:
                body += "\n" + " · ".join(links)
            GH.post_fresh_comment(tag="llvm-coverage", body=body)
        else:
            print("Not a PR run, skipping GitHub coverage comment")

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
                "changed_lines_total": d.get("changed_lines_total", 0),
                "changed_lines_covered": d.get("changed_lines_covered", 0),
                "changed_lines_cov": d.get("changed_lines_cov", 0.0),
                "coverage_report_url": d["coverage_report_url"],
                "diff_coverage_report_url": d.get("diff_coverage_report_url", ""),
                "uncovered_code_url": uncovered_code_url,
            },
        )
    except Exception:
        print("ERROR: Failed to post coverage comment or insert into CIDB")
        traceback.print_exc()


if __name__ == "__main__":
    check()
