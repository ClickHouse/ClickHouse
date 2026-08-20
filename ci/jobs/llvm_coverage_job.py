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
from ci.defs.defs import LLVM_ARTIFACTS_LIST, S3_REPORT_BUCKET_HTTP_ENDPOINT

CURRENT_DIR = Utils.cwd()
TEMP_DIR = f"{CURRENT_DIR}/ci/tmp/"


def expected_profile_files(artifact_names) -> list[str]:
    """Profile filenames this run must merge, one per coverage shard artifact.

    Every producer names its profile after its own artifact
    (`<artifact>.profdata`), so the filename carries the shard identity and
    completeness is a plain set comparison against this list.
    """
    return sorted(f"{name}.profdata" for name in artifact_names)


def present_profile_files(directory: str) -> list[str]:
    """Profile filenames present in `directory`.

    Must be snapshotted BEFORE the aggregate merge runs: the merge writes its
    own merged.profdata into the same directory.
    """
    if not os.path.isdir(directory):
        return []
    return sorted(
        name
        for name in os.listdir(directory)
        if name.endswith(".profdata")
        and os.path.isfile(os.path.join(directory, name))
    )


def missing_profile_files(expected: list[str], present: list[str]) -> list[str]:
    """Expected shard profiles that did not arrive.

    Extra files in `present` (e.g. a stale merged.profdata) are deliberately not
    an error: the merge is given exactly the expected list, so they are never
    folded into the total.
    """
    return sorted(set(expected) - set(present))


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


COVERAGE_DROP_TOLERANCE = 0.3


def coverage_drop(baseline_cov: float, current_cov: float) -> float:
    """Return the line coverage drop in pp, rounded to two decimals.

    In practice lcov reports these percentages with one decimal, so subtracting
    two of them can land just above the tolerance:
    `84.4 - 84.1 == 0.30000000000001137`, which made a drop exactly equal to the
    tolerance fail the check below. Rounding to two decimals is finer than the
    reported precision, so a drop lcov can actually express is never masked.
    """
    return round(baseline_cov - current_cov, 2)


def coverage_degraded(drop: float) -> bool:
    """A drop equal to the tolerance is allowed, as the gate's message states."""
    return drop > COVERAGE_DROP_TOLERANCE


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
                f"gh api 'repos/ClickHouse/ClickHouse/commits?sha={merge_base}&per_page=100' -q '.[].sha'",
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

    # A verdict may only be derived from a COMPLETE measurement: all expected
    # shard profiles present and merged all-or-nothing. On any shortfall the job
    # reports SKIPPED with the reason and withholds every comparative output -
    # in particular llvm_coverage.info, so that "an .info exists for a master
    # commit" keeps meaning "that commit's measurement was complete" (the diff
    # gate selects its baseline by exactly that existence test), and the CI DB
    # row, so an incomplete master run cannot poison the baseline series.
    _expected_profiles = expected_profile_files(LLVM_ARTIFACTS_LIST)
    _present_profiles = present_profile_files(TEMP_DIR)
    _missing_profiles = missing_profile_files(_expected_profiles, _present_profiles)
    print(
        f"Coverage shard profiles: expected {len(_expected_profiles)}, "
        f"present {len(_present_profiles)}, missing {len(_missing_profiles)}"
    )

    measurement_ok = True
    skip_reason = ""
    if _missing_profiles:
        measurement_ok = False
        skip_reason = (
            f"incomplete coverage measurement: {len(_missing_profiles)} of "
            f"{len(_expected_profiles)} shard profiles are missing: "
            f"{', '.join(_missing_profiles)}"
        )
        merge_res = Result.create_from(
            name="Merge LLVM Coverage Profiles",
            status=Result.Status.SKIPPED,
            info=skip_reason,
        )
        merge_res.set_comment(skip_reason)
    else:
        _merge_env = f"MERGE_PROFDATA_FILES={shlex.quote(' '.join(_expected_profiles))}"
        merge_res = Result.from_commands_run(
            name="Merge LLVM Coverage Profiles",
            command=[f"{_merge_env} bash ci/jobs/scripts/merge_llvm_coverage.sh merge"],
        )
        _merge_status_file = Path(TEMP_DIR) / "merge_profdata.status"
        _merge_status = (
            _merge_status_file.read_text().strip()
            if _merge_status_file.exists()
            else ""
        )
        if merge_res.is_ok() and _merge_status == "ok":
            pass
        elif merge_res.is_ok():
            # The merge ran and rejected an input (--failure-mode=any): an
            # incomplete measurement, not a tooling failure.
            measurement_ok = False
            skip_reason = (
                "the aggregate profile merge rejected an invalid shard profile, "
                "so this run has no complete measurement (see the merge step log)"
            )
            merge_res.set_status(Result.Status.SKIPPED)
            merge_res.set_info(skip_reason)
            merge_res.set_comment(skip_reason)
        else:
            # The merge step itself broke (missing tool, bad invocation): a
            # tooling failure, so merge_res stays FAIL and the job reddens.
            measurement_ok = False
            skip_reason = "the aggregate profile merge step failed"
    results.append(merge_res)
    if not measurement_ok:
        print(f"NOTE: {skip_reason}")

    if measurement_ok:
        gen_report_res = Result.from_commands_run(
            name="Generate LLVM Coverage Report",
            command=["bash ci/jobs/scripts/merge_llvm_coverage.sh report"],
        )
        # Compress and attach the full HTML report archive + files to the generate result.
        # Keeping files/assets inside the same sub-Result ensures upload_result_files_to_s3
        # computes common_root = llvm_coverage_html_report/, so relative links stay intact.
        # The directory is absent when the report phase failed; that failure is
        # already RED, so do not compound it with an exception here.
        if Path(f"{TEMP_DIR}/llvm_coverage_html_report").exists():
            Utils.compress_gz(
                f"{TEMP_DIR}/llvm_coverage_html_report",
                f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
            )
            gen_report_res.files.append(f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz")
            _html_files, _html_assets = collect_html_report_files("llvm_coverage_html_report")
            gen_report_res.files.extend(_html_files)
            gen_report_res.assets.extend(_html_assets)
    else:
        gen_report_res = Result.create_from(
            name="Generate LLVM Coverage Report",
            status=Result.Status.SKIPPED,
            info=skip_reason,
        )
        gen_report_res.set_comment(skip_reason)
    results.append(gen_report_res)

    if not is_master_branch and not measurement_ok:
        # No verdict may be produced from an incomplete measurement: skip the
        # comparison, the uncovered-code analysis, the GitHub comment and the
        # CI DB row. SKIPPED counts as OK, so an infra shortfall the PR author
        # cannot act on does not block the PR.
        _skip_msg = f"Coverage comparison skipped: {skip_reason}"
        print(_skip_msg)
        diff_res = Result.create_from(
            name="Generate LLVM Coverage Diff Report",
            status=Result.Status.SKIPPED,
            info=_skip_msg,
        )
        diff_res.set_comment(_skip_msg)
        results.append(diff_res)
        print_res = Result.create_from(
            name="Print Uncovered Code",
            status=Result.Status.SKIPPED,
            info=_skip_msg,
        )
        print_res.set_comment(_skip_msg)
        results.append(print_res)
    elif not is_master_branch:
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
            # Baseline coverage from the primary master run.
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

            _drop = coverage_drop(b_line_cov, c_line_cov)
            if coverage_degraded(_drop):
                _failure_msg = (
                    f"Coverage degraded: master {b_line_cov:.2f}% → PR {c_line_cov:.2f}%"
                    f" (dropped {_drop:.2f} pp, tolerance {COVERAGE_DROP_TOLERANCE} pp)"
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

            # Only write coverage_comment.json (and thus post a GitHub comment) when
            # the diff HTML report was generated (i.e. C/C++ source files changed).
            # Tests-only PRs never reach this job at all - the coverage family is
            # auto-skipped for them (see filter_job.py) since the compiled binary,
            # and therefore coverage, cannot have moved.
            _has_coverage_data = _diff_ran
            if not _has_coverage_data:
                print("No coverage-relevant changes detected (no C/C++ source changes) — skipping coverage comment.")
            else:
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
                    # The uncovered-code log is produced only when print_uncovered_code.py
                    # actually ran (i.e. C/C++ source files changed). For tests-only PRs
                    # the log doesn't exist on S3, so don't surface a 404 link.
                    "uncovered_code_url": uncovered_code_url if _diff_inputs_exist else "",
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
        if not is_local_run and not measurement_ok:
            # The post-hook inserts this row into the coverage CI DB table,
            # which is the series later baselines and trends read. An
            # incomplete master measurement must not enter it.
            print(
                "This master run's coverage measurement is incomplete, "
                "skipping the CI DB row so it cannot poison the baseline series."
            )
        elif not is_local_run:
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
        # Only publish a link when the artifact it addresses exists: on an
        # incomplete measurement no report is generated, and an unconditional
        # append would point the intended green SKIPPED result at a 404.
        if Path(f"{TEMP_DIR}/llvm_coverage_html_report/index.html").exists():
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html"
            )
        if _diff_ran:
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            )

    archives = [
        a
        for a in [
            f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
            f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz" if _diff_ran else None,
        ]
        if a and Path(a).exists()
    ]

    _job_info = "LLVM Coverage Job Completed"
    if not measurement_ok:
        _job_info = f"{_job_info} ({skip_reason})"

    Result.create_from(
        results=results,
        files=archives,
        links=report_links,
        info=_job_info,
    ).complete_job(disable_attached_files_sorting=True)
