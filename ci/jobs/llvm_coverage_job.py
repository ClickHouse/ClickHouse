import json
import os
import re
import shlex
import shutil
from datetime import datetime, timezone
from pathlib import Path
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils
from ci.defs.defs import S3_REPORT_BUCKET_HTTP_ENDPOINT

CURRENT_DIR = Utils.cwd()
TEMP_DIR = f"{CURRENT_DIR}/ci/tmp/"


def get_lcov_summary(
    info_file_path: str,
) -> tuple[
    tuple[float, int, int],
    tuple[float, int, int],
    tuple[float, int, int],
]:
    """Return ((pct, hit, total), ...) for lines, functions, and branches.

    Each inner tuple contains the coverage percentage, the number of covered
    items (hit), and the total number of items.  Raw counts allow callers to
    compute precise deltas (e.g. "+55 lines covered") that round-tripping
    through a percentage would lose.
    """
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

    def extract_metric(metric: str) -> tuple[float, int, int]:
        # lcov --summary format: "  lines......: 55.23% (12345 of 22345 lines)"
        match = re.search(
            rf"^\s*{metric}\.*:\s*([0-9]+(?:\.[0-9]+)?)%\s+\((\d+) of (\d+)",
            output,
            re.MULTILINE,
        )
        if match:
            return float(match.group(1)), int(match.group(2)), int(match.group(3))
        if re.search(rf"^\s*{metric}\.*:\s*no data found", output, re.MULTILINE):
            raise ValueError(
                f"lcov summary contains no data for '{metric}'. "
                "Make sure you run lcov with --branch-coverage when you need branch stats."
            )
        raise ValueError(
            f"Failed to parse '{metric}' from lcov output:\n{output}"
        )

    return (
        extract_metric("lines"),
        extract_metric("functions"),
        extract_metric("branches"),
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


def get_git_info() -> tuple[str, list[str], str, str, str, int]:
    # Get git info from Info singleton, if not present, get it from shell commands
    # Returns: current_commit_sha, master_track_commits, branch, base_branch, repo_name, pr_number
    info = Info()

    current_commit_sha = info.sha
    if current_commit_sha is None:
        current_commit_sha = Shell.get_output(
            "git rev-parse HEAD", verbose=True
        ).strip()

    # master_track_commits is a list of master-side commits (nearest first) stored by
    # store_data.py.  The first entry doubles as the base commit for diff comparisons.
    # In a local run (or when the hook has not populated the key) we fall back to
    # deriving the merge base via the GitHub API and walking back 30 commits from it.
    master_track_commits: list[str] = info.get_kv_data("master_track_commits_sha") or []
    if not master_track_commits:
        merge_base = Shell.get_output(
            f"gh api repos/ClickHouse/ClickHouse/compare/master...{current_commit_sha} -q .merge_base_commit.sha",
            verbose=True,
        ).strip()
        if merge_base:
            raw = Shell.get_output(
                f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={merge_base}&per_page=30' -q '.[].sha'",
                verbose=True,
            )
            master_track_commits = raw.splitlines()

    branch = (
        info.git_branch
        or Shell.get_output("git branch --show-current", verbose=True).strip()
    )
    base_branch = (
        info.base_branch
        or Shell.get_output(
            "gh pr view --json baseRefName --template '{{.baseRefName}}'", verbose=True
        ).strip().replace("origin/", "")
        or "master"
    )
    repo_name = (
        info.repo_name
        or Shell.get_output(
            "basename -s .git `git config --get remote.origin.url`", verbose=True
        ).strip()
    )
    if info.pr_number > 0:
        pr_number = info.pr_number
    else:
        _gh_out = Shell.get_output(
            "gh pr view --json number -q .number", verbose=True
        ).strip()
        pr_number = int(_gh_out) if _gh_out else 0
    return (
        current_commit_sha,
        master_track_commits,
        branch,
        base_branch,
        repo_name,
        pr_number,
    )


if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = CURRENT_DIR

    is_local_run = Info().is_local_run

    (
        current_commit_sha,
        master_track_commits,
        branch,
        base_branch,
        repo_name,
        pr_number,
    ) = get_git_info()

    # Use the nearest master-side commit as the base for diff comparisons.
    base_commit_sha = master_track_commits[0] if master_track_commits else ""

    os.environ["BRANCH"] = branch
    os.environ["CURRENT_COMMIT"] = current_commit_sha
    os.environ["BASE_BRANCH"] = base_branch
    os.environ["BASE_COMMIT"] = base_commit_sha
    os.environ["REPO_NAME"] = repo_name
    os.environ["PR_NUMBER"] = str(pr_number)
    os.environ["PREV_30_COMMITS"] = ",".join(master_track_commits)

    is_master_branch = branch == "master"
    _diff_ran = False

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

        # The diff script exits 0 without running genhtml when no C/C++ files changed.
        # Use the presence of its output directory as the authoritative indicator.
        _diff_report_dir = Path(TEMP_DIR) / "llvm_coverage_diff_html_report"
        _diff_ran = _diff_report_dir.exists()

        b_line_cov = c_line_cov = b_function_cov = c_function_cov = b_branch_cov = c_branch_cov = delta = 0.0
        b_line_hit = b_line_total = c_line_hit = c_line_total = 0
        b_func_hit = b_func_total = c_func_hit = c_func_total = 0
        b_branch_hit = b_branch_total = c_branch_hit = c_branch_total = 0

        if _diff_ran:
            # Baseline coverage for the current branch (from the merged report)
            (b_line_cov, b_line_hit, b_line_total), \
            (b_function_cov, b_func_hit, b_func_total), \
            (b_branch_cov, b_branch_hit, b_branch_total) = get_lcov_summary(
                f"{TEMP_DIR}/base_llvm_coverage.info"
            )

            # Current coverage for the current branch
            (c_line_cov, c_line_hit, c_line_total), \
            (c_function_cov, c_func_hit, c_func_total), \
            (c_branch_cov, c_branch_hit, c_branch_total) = get_lcov_summary(
                f"{TEMP_DIR}/llvm_coverage.info"
            )

            delta = c_line_cov - b_line_cov
            print(f"Baseline coverage : {b_line_cov:.2f}%")
            print(f"Current coverage  : {c_line_cov:.2f}%")
            print(f"Delta             : {delta:+.2f}%")

            TOLERANCE = 0.3
            if b_line_cov - c_line_cov > TOLERANCE:
                _failure_msg = (
                    f"Coverage degraded: master {b_line_cov:.2f}% → PR {c_line_cov:.2f}%"
                    f" (dropped {b_line_cov - c_line_cov:.2f} pp, tolerance {TOLERANCE} pp)"
                )
                print(_failure_msg)
                diff_res.info = _failure_msg
                diff_res.set_comment(_failure_msg)
                diff_res.set_failed()
            else:
                print(f"Coverage did not degrade beyond tolerance (delta {delta:+.2f}%).")

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
        else:
            print("No C/C++ source files changed — differential coverage report was not generated.")

        results.append(diff_res)

        # Generate report for changed blocks only
        _print_log = f"{TEMP_DIR}{Utils.normalize_string('Print Uncovered Code')}.log"
        _diff_inputs_exist = (
            Path(TEMP_DIR + "changes.diff").exists()
            and Path(TEMP_DIR + "current.changed.info").exists()
        )
        if _diff_inputs_exist:
            Shell.run(
                f"python3 ci/jobs/scripts/print_uncovered_code.py 2>&1 | tee {_print_log}",
                verbose=True,
            )
            print_res = Result.from_fs("Print Uncovered Code")
        else:
            msg = "No C/C++ source files changed — skipping uncovered code analysis."
            print(msg)
            print_res = Result.create_from(
                name="Print Uncovered Code",
                status=Result.Status.OK,
                info=msg,
            )
            print_res.set_comment(msg)
        # Append high-precision hit/total counts to the log so they are visible
        # in the artifact without cluttering the GitHub comment.
        if _diff_ran:
            with open(_print_log, "a") as _f:
                _f.write(
                    f"\n--- Coverage counts ---\n"
                    f"Lines     : baseline {b_line_hit:,}/{b_line_total:,}"
                    f"  ->  current {c_line_hit:,}/{c_line_total:,}"
                    f"  (delta {c_line_hit - b_line_hit:+,} / {c_line_total - b_line_total:+,})\n"
                    f"Functions : baseline {b_func_hit:,}/{b_func_total:,}"
                    f"  ->  current {c_func_hit:,}/{c_func_total:,}"
                    f"  (delta {c_func_hit - b_func_hit:+,} / {c_func_total - b_func_total:+,})\n"
                    f"Branches  : baseline {b_branch_hit:,}/{b_branch_total:,}"
                    f"  ->  current {c_branch_hit:,}/{c_branch_total:,}"
                    f"  (delta {c_branch_hit - b_branch_hit:+,} / {c_branch_total - b_branch_total:+,})\n"
                )
        if _diff_inputs_exist:
            print_res.files.append(_print_log)
        results.append(print_res)

        if not is_local_run:
            # Construct S3 artifact URLs from the known upload path structure:
            #   HTML files/assets → https://<endpoint>/<s3_prefix>/<normalize(job)>/<normalize(sub_result)>/<rel_path>
            #   log files         → https://<endpoint>/<s3_prefix>/<normalize(job)>/<normalize(result)>/<log_basename>
            _s3_prefix = (
                f"PRs/{pr_number}/{current_commit_sha}"
                if pr_number > 0
                else f"REFs/{branch}/{current_commit_sha}"
            )
            _s3_base = f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_s3_prefix}"
            _log_name = f"{Utils.normalize_string(print_res.name)}.log"
            uncovered_code_url = f"{_s3_base}/llvm_coverage/{Utils.normalize_string(print_res.name)}/{_log_name}"

            _diff_url = f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            _pr_changed_lines_info = print_res.ext.get("comment", "")
            _changed_lines_total = print_res.ext.get("changed_lines_total", 0)
            _changed_lines_covered = print_res.ext.get("changed_lines_covered", 0)
            _changed_lines_cov = print_res.ext.get("changed_lines_cov", 0.0)

            _lbc_lines = print_res.ext.get("lbc_lines", 0)
            _lbc_fns = print_res.ext.get("lbc_fns", 0)

            # Only write coverage_comment.json (and thus post a GitHub comment) when
            # there is something coverage-related to report: either the diff HTML report
            # was generated (C++ source files changed) or LBC was detected (tests removed).
            # Pure non-C++ PRs (scripts, Docker, configs) produce neither and should not
            # generate a comment.
            _has_coverage_data = _diff_ran or _lbc_lines > 0 or _lbc_fns > 0
            if not _has_coverage_data:
                print("No C/C++ source files changed and no lost baseline coverage — skipping coverage comment.")
            else:
                # When _diff_ran is False but LBC was found (test-only removal), fetch
                # the global percentages from the .info files that were downloaded during
                # the diff script run for LBC comparison.
                _base_info = f"{TEMP_DIR}/base_llvm_coverage.info"
                _curr_info = f"{TEMP_DIR}/llvm_coverage.info"
                if not _diff_ran and Path(_base_info).exists() and Path(_curr_info).exists():
                    try:
                        (b_line_cov, b_line_hit, b_line_total), \
                        (b_function_cov, b_func_hit, b_func_total), \
                        (b_branch_cov, b_branch_hit, b_branch_total) = get_lcov_summary(_base_info)
                        (c_line_cov, c_line_hit, c_line_total), \
                        (c_function_cov, c_func_hit, c_func_total), \
                        (c_branch_cov, c_branch_hit, c_branch_total) = get_lcov_summary(_curr_info)
                        delta = c_line_cov - b_line_cov
                    except Exception as e:
                        print(f"Warning: could not compute global coverage percentages: {e}")

                _comment_data = {
                    # GitHub comment fields
                    "b_line_cov": b_line_cov,
                    "c_line_cov": c_line_cov,
                    "b_function_cov": b_function_cov,
                    "c_function_cov": c_function_cov,
                    "b_branch_cov": b_branch_cov,
                    "c_branch_cov": c_branch_cov,
                    "b_line_hit": b_line_hit,
                    "b_line_total": b_line_total,
                    "c_line_hit": c_line_hit,
                    "c_line_total": c_line_total,
                    "b_func_hit": b_func_hit,
                    "b_func_total": b_func_total,
                    "c_func_hit": c_func_hit,
                    "c_func_total": c_func_total,
                    "b_branch_hit": b_branch_hit,
                    "b_branch_total": b_branch_total,
                    "c_branch_hit": c_branch_hit,
                    "c_branch_total": c_branch_total,
                    "pr_changed_lines_info": _pr_changed_lines_info,
                    "changed_lines_total": _changed_lines_total,
                    "changed_lines_covered": _changed_lines_covered,
                    "changed_lines_cov": _changed_lines_cov,
                    "diff_url": _diff_url if _diff_ran else "",
                    "uncovered_code_url": uncovered_code_url,
                    # CIDB fields
                    "check_start_time": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "pull_request_number": pr_number,
                    "commit_sha": current_commit_sha,
                    "base_commit_sha": base_commit_sha,
                    "branch": branch,
                    "base_branch": base_branch,
                    "status": diff_res.status,
                    "delta_line_cov": delta,
                    "coverage_report_url": f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html",
                    "diff_coverage_report_url": _diff_url if _diff_ran else "",
                }
                with open(f"{TEMP_DIR}/coverage_comment.json", "w") as f:
                    json.dump(_comment_data, f)
        else:
            print("Local run, skipping CI DB update with coverage results")
    else:
        print("On master branch, skipping diff coverage generation")
        if not is_local_run:
            try:
                (m_line_cov, m_line_hit, m_line_total), \
                (m_function_cov, m_func_hit, m_func_total), \
                (m_branch_cov, m_branch_hit, m_branch_total) = get_lcov_summary(
                    f"{TEMP_DIR}/llvm_coverage.info"
                )
                print(f"Master coverage: lines={m_line_cov:.2f}% ({m_line_hit}/{m_line_total}) functions={m_function_cov:.2f}% ({m_func_hit}/{m_func_total}) branches={m_branch_cov:.2f}% ({m_branch_hit}/{m_branch_total})")
                _s3_prefix = f"REFs/{branch}/{current_commit_sha}"
                _s3_base = f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_s3_prefix}"
                _master_data = {
                    "check_start_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "pull_request_number": 0,
                    "commit_sha": current_commit_sha,
                    "base_commit_sha": "",
                    "branch": branch,
                    "base_branch": base_branch,
                    "status": gen_report_res.status,
                    "b_line_cov": 0.0,
                    "c_line_cov": m_line_cov,
                    "b_function_cov": 0.0,
                    "c_function_cov": m_function_cov,
                    "b_branch_cov": 0.0,
                    "c_branch_cov": m_branch_cov,
                    "b_line_hit": 0,
                    "b_line_total": 0,
                    "c_line_hit": m_line_hit,
                    "c_line_total": m_line_total,
                    "b_func_hit": 0,
                    "b_func_total": 0,
                    "c_func_hit": m_func_hit,
                    "c_func_total": m_func_total,
                    "b_branch_hit": 0,
                    "b_branch_total": 0,
                    "c_branch_hit": m_branch_hit,
                    "c_branch_total": m_branch_total,
                    "delta_line_cov": 0.0,
                    "coverage_report_url": f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html",
                    "diff_coverage_report_url": "",
                    "uncovered_code_url": "",
                    "pr_changed_lines_info": "",
                    "diff_url": "",
                }
                with open(f"{TEMP_DIR}/coverage_comment.json", "w") as f:
                    json.dump(_master_data, f)
            except Exception as e:
                print(f"Warning: failed to compute master coverage stats: {e}")

    # Add direct S3 links to both HTML reports in the main job result.
    # HTML files are uploaded within the corresponding generate sub-result;
    # the URL is deterministic: llvm_coverage/<normalize(sub_result_name)>/<filename>.
    report_links = []
    if not is_local_run:
        _s3_prefix = (
            f"PRs/{pr_number}/{current_commit_sha}"
            if pr_number > 0
            else f"REFs/{branch}/{current_commit_sha}"
        )
        _s3_base = f"https://{S3_REPORT_BUCKET_HTTP_ENDPOINT}/{_s3_prefix}"
        report_links.append(
            f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html"
        )
        if _diff_ran:
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            )

    archives = [f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz"]
    if _diff_ran:
        archives.append(f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz")

    Result.create_from(
        results=results,
        files=archives,
        links=report_links,
        info="LLVM Coverage Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
