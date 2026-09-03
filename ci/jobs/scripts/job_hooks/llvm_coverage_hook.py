import json
import re
import traceback
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.gh import GH
from ci.praktika.info import Info

_COVERAGE_TAG_START = "<!-- CI automatic comment start :coverage: -->"
_COVERAGE_TAG_END = "<!-- CI automatic comment end :coverage: -->"


def preserved_coverage_lines(section: str) -> str:
    """The last complete run's rendered numbers, extracted from the comment's
    current coverage section.

    Drops the section's own header and any stale-measurement warning, so
    consecutive skipped runs replace the warning instead of stacking warnings.
    The "Measured on commit" line is kept: it is what attributes the preserved
    numbers to the commit that produced them.
    """
    kept = [
        line
        for line in section.splitlines()
        if not line.startswith("### LLVM Coverage Report")
        and not line.startswith("⚠️")
    ]
    return "\n".join(kept).strip()


def parse_paginated_arrays(output: str) -> list:
    """Flatten the output of `gh api --paginate --jq '[...]'`, which emits one
    JSON array per page, concatenated - a single json.loads breaks on page 2."""
    decoder = json.JSONDecoder()
    items = []
    text = (output or "").strip()
    idx = 0
    while idx < len(text):
        page, end = decoder.raw_decode(text, idx)
        items.extend(page)
        idx = end
        while idx < len(text) and text[idx].isspace():
            idx += 1
    return items


def current_coverage_section(repo: str, pr: int) -> str:
    """The coverage section of the existing updateable PR comment, "" if absent."""
    cmd = (
        f'gh api -H "Accept: application/vnd.github.v3+json" '
        f'"/repos/{repo}/issues/{pr}/comments" --jq \'[.[] | .body]\' --paginate'
    )
    output = GH.get_output_with_retries(cmd, verbose=False)
    try:
        bodies = parse_paginated_arrays(output)
    except (json.JSONDecodeError, TypeError):
        return ""
    for body in bodies:
        if _COVERAGE_TAG_START in body and _COVERAGE_TAG_END in body:
            match = re.search(
                f"{re.escape(_COVERAGE_TAG_START)}(.*){re.escape(_COVERAGE_TAG_END)}",
                body,
                re.DOTALL,
            )
            if match:
                return match.group(1).strip()
    return ""


def check():
    info = Info()

    comment_file = Path("./ci/tmp/coverage_comment.json")
    if not comment_file.exists():
        print(f"Coverage comment data not found at {comment_file}, skipping")
        return

    try:
        with open(comment_file) as f:
            d = json.load(f)

        if "skipped_reason" in d:
            # This run produced no complete measurement, so there are no new
            # numbers and no CI DB row. The comment section is still updated:
            # otherwise it would keep showing the previous commit's numbers
            # with nothing to say they are stale.
            if info.pr_number > 0:
                sha = d.get("commit_sha", "")
                body = (
                    f"### LLVM Coverage Report\n\n"
                    f"⚠️ No coverage measurement for commit {sha[:8]}: {d['skipped_reason']}.\n"
                )
                previous = preserved_coverage_lines(
                    current_coverage_section(info.repo_name, info.pr_number)
                )
                if previous:
                    body += f"\n{previous}\n"
                GH.post_updateable_comment(
                    comment_tags_and_bodies={"coverage": body}, only_update=True
                )
            else:
                print("Not a PR run, skipping GitHub coverage comment")
            return

        b_line_cov = d["b_line_cov"]
        c_line_cov = d["c_line_cov"]
        b_function_cov = d["b_function_cov"]
        c_function_cov = d["c_function_cov"]
        b_branch_cov = d["b_branch_cov"]
        c_branch_cov = d["c_branch_cov"]
        d.get("b_line_hit", 0)
        d.get("b_line_total", 0)
        d.get("c_line_hit", 0)
        d.get("c_line_total", 0)
        d.get("b_func_hit", 0)
        d.get("b_func_total", 0)
        d.get("c_func_hit", 0)
        d.get("c_func_total", 0)
        d.get("b_branch_hit", 0)
        d.get("b_branch_total", 0)
        d.get("c_branch_hit", 0)
        d.get("c_branch_total", 0)
        pr_changed_lines_info = d.get("pr_changed_lines_info", "")
        diff_url = d.get("diff_url", "")
        uncovered_code_url = d.get("uncovered_code_url", "")

        if info.pr_number > 0:
            # The "Measured on commit" line attributes the numbers: when a later
            # run has no complete measurement, the hook keeps these lines in the
            # comment under a stale-measurement warning (see the skipped_reason
            # branch above), and this line is what dates them.
            body = (
                f"### LLVM Coverage Report\n\n"
                f"Measured on commit {d.get('commit_sha', '')[:8]}.\n\n"
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
            GH.post_updateable_comment(
                comment_tags_and_bodies={"coverage": body}, only_update=True
            )
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
