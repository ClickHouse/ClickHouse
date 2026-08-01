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
from ci.jobs.scripts import llvm_coverage_completeness as completeness

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

    # Which shard profiles this run expects, and which actually arrived. Snapshot
    # the present set BEFORE the merge runs: the merge writes merged.profdata into
    # the very directory being listed.
    _expected_artifacts = list(LLVM_ARTIFACTS_LIST)
    _present_profiles = completeness.present_profiles(TEMP_DIR)
    _merge_inputs = completeness.merge_inputs(_expected_artifacts, _present_profiles)
    print(f"Coverage shard profiles expected: {len(_expected_artifacts)}, present: {len(_present_profiles)}")

    # Merge and report are separate steps. A failed MERGE means "no complete
    # measurement", which is reported as SKIPPED further down; a failed REPORT is a
    # tooling failure and must stay RED. praktika collapses a step's exit code to a
    # boolean, so the merge status is passed back in a marker file instead.
    if not _merge_inputs:
        # An empty input set is the limit of the same tolerated case as one absent
        # shard, so it reports SKIPPED instead of reddening. The script is not
        # invoked: its guard cannot tell an empty value from an omitted one, and it
        # must keep refusing the latter to stay safe against an unbounded glob. It
        # writes no marker either, so the marker read is skipped rather than
        # satisfied with a fabricated one.
        _merge_msg = "no shard coverage profile arrived, so there is nothing to merge"
        merge_res = Result.create_from(
            name="Merge LLVM Coverage Profiles",
            status=Result.Status.SKIPPED,
            info=_merge_msg,
        )
        merge_res.set_comment(_merge_msg)
        _merge_ok = False
    else:
        _merge_env = f"MERGE_PROFDATA_FILES={shlex.quote(' '.join(_merge_inputs))}"
        merge_res = Result.from_commands_run(
            name="Merge LLVM Coverage Profiles",
            command=[f"{_merge_env} bash ci/jobs/scripts/merge_llvm_coverage.sh merge"],
        )
        _merge_status_file = Path(TEMP_DIR) / "merge_profdata.status"
        _merge_ok = (
            merge_res.is_ok()
            and _merge_status_file.exists()
            and _merge_status_file.read_text().strip() == "ok"
        )
    results.append(merge_res)
    if not _merge_ok:
        print("The aggregate coverage merge did not produce a usable profile.")

    gen_report_res = Result.from_commands_run(
        name="Generate LLVM Coverage Report",
        command=["bash ci/jobs/scripts/merge_llvm_coverage.sh report"],
    )

    # Our own completeness metadata, published beside llvm_coverage.info so that a
    # later PR comparing against this commit can tell whether this measurement was
    # complete. Written even when incomplete - "incomplete" is exactly the fact the
    # consumer needs.
    _sidecar = completeness.build_sidecar(
        _expected_artifacts,
        _present_profiles,
        info_path=f"{TEMP_DIR}/llvm_coverage.info",
        merge_ok=_merge_ok,
    )
    completeness.write_sidecar(f"{TEMP_DIR}/{completeness.SIDECAR_BASENAME}", _sidecar)
    if not _sidecar["complete"]:
        print(
            "This run's coverage measurement is INCOMPLETE: "
            f"missing={_sidecar['missing']} unexpected={_sidecar['unexpected']} merge_ok={_merge_ok}"
        )

    # Compress and attach the full HTML report archive + files to the generate result.
    # Keeping files/assets inside the same sub-Result ensures upload_result_files_to_s3
    # computes common_root = llvm_coverage_html_report/, so relative links stay intact.
    # The report directory is absent when the merge produced no profile.
    if Path(f"{TEMP_DIR}/llvm_coverage_html_report").exists():
        Utils.compress_gz(
            f"{TEMP_DIR}/llvm_coverage_html_report",
            f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
        )
        gen_report_res.files.append(f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz")
        _html_files, _html_assets = collect_html_report_files("llvm_coverage_html_report")
        gen_report_res.files.extend(_html_files)
        gen_report_res.assets.extend(_html_assets)
    if not _sidecar["complete"]:
        # The full report is still published: it is a genuine measurement of what
        # DID run and the best artifact for finding out which shard went missing.
        # It is labelled so it cannot be read as a complete measurement.
        _n_present = len(_expected_artifacts) - len(_sidecar["missing"])
        _banner = (
            f"partial measurement: {_n_present} of {len(_expected_artifacts)} shards"
        )
        gen_report_res.info = f"{gen_report_res.info}\n{_banner}" if gen_report_res.info else _banner
    results.append(gen_report_res)

    _measurement_comparable = True
    _incomparable_reason = ""

    if not is_master_branch:
        # Both sides must be complete measurements of the same artifact manifest
        # before any number derived from them may be published.
        #
        # The baseline sidecar and the selected-base marker are produced BY the
        # differential script, so they may only be read after it has run.
        #
        # The current side is decided BEFORE it. An absent llvm_coverage.info trips
        # that script's own precondition, which exits 1 and turns the sub-result
        # FAIL; its output directory is then missing too, so the SKIPPED override
        # further down is never reached and the whole job reddens - exactly the
        # outcome a failed aggregate merge is meant to report as SKIPPED. The
        # sidecar's own causes are short-circuited here for a different reason:
        # running the script on a measurement already known incomparable spends
        # genhtml on it and prints a delta the very next line says was not judged.
        # Baseline-side causes cannot be known yet, because that script is what
        # fetches the baseline, so they stay with the override.
        _have_own_info = Path(f"{TEMP_DIR}/llvm_coverage.info").exists()
        _own_side_reason = completeness.current_side_reason(_sidecar)
        if not _have_own_info or _own_side_reason:
            _measurement_comparable = False
            _incomparable_reason = (
                _own_side_reason
                if _have_own_info
                else "this run published no coverage data, so there is nothing to compare"
            )
            # The script never ran, so it wrote no marker and there is nothing to read.
            _selected_base_commit = ""
            diff_res = Result.create_from(
                name="Generate LLVM Coverage Diff Report",
                status=Result.Status.SKIPPED,
                info=f"Coverage comparison skipped: {_incomparable_reason}",
            )
            diff_res.set_comment(f"Coverage comparison skipped: {_incomparable_reason}")
        else:
            diff_res = Result.from_commands_run(
                name="Generate LLVM Coverage Diff Report",
                command=["bash ci/jobs/scripts/generate_diff_coverage_report.sh"],
            )
            _baseline_sidecar = completeness.read_sidecar(
                f"{TEMP_DIR}/base_llvm_coverage.meta.json"
            )
            _selected_base_file = Path(TEMP_DIR) / "selected_base_commit.txt"
            _selected_base_commit = (
                _selected_base_file.read_text().strip()
                if _selected_base_file.exists()
                else ""
            )
            _measurement_comparable, _incomparable_reason = completeness.comparable(
                _sidecar,
                _baseline_sidecar,
                baseline_info_path=f"{TEMP_DIR}/base_llvm_coverage.info",
            )
        if not _measurement_comparable:
            print(f"Coverage comparison skipped: {_incomparable_reason}")
        elif _selected_base_commit:
            print(f"Comparing against baseline commit {_selected_base_commit}")

        # The diff script exits 0 without running genhtml when no C/C++ files changed.
        # Use the presence of its output directory as the authoritative indicator.
        _diff_report_dir = Path(TEMP_DIR) / "llvm_coverage_diff_html_report"
        _diff_ran = _diff_report_dir.exists()

        b_line_cov = c_line_cov = b_function_cov = c_function_cov = b_branch_cov = c_branch_cov = delta = 0.0
        b_line_hit = b_line_total = c_line_hit = c_line_total = 0
        b_func_hit = b_func_total = c_func_hit = c_func_total = 0
        b_branch_hit = b_branch_total = c_branch_hit = c_branch_total = 0

        if _diff_ran:
            # A baseline-side cause is only knowable after the script has run, so the
            # script legitimately ran and this block is reached with the measurement
            # already known incomparable. Parsing the two summaries and printing a
            # delta here would put three numbers between two abstention notices,
            # which is the reading confusion this gate exists to remove. The
            # zero-initialisations above keep the unparsed variables safe.
            if _measurement_comparable:
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
            # Unlike the full report, the DIFFERENTIAL report renders per-line deltas
            # against the baseline, so it belongs with the numbers: when the two sides
            # are not comparable its content is as fabricated as the verdict would be.
            if _measurement_comparable:
                Utils.compress_gz(
                    f"{TEMP_DIR}/llvm_coverage_diff_html_report",
                    f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz",
                )
                diff_res.files.append(f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz")
                # Copy index.html to index_diff.html so the diff entry-point has a
                # unique name in S3 links. The original index.html is kept as an asset
                # so that relative links inside the report continue to work.
                _diff_index = Path(TEMP_DIR) / "llvm_coverage_diff_html_report" / "index.html"
                _diff_index_copy = _diff_index.parent / "index_diff.html"
                if _diff_index.exists():
                    shutil.copy2(_diff_index, _diff_index_copy)
                _diff_files, _diff_assets = collect_html_report_files(
                    "llvm_coverage_diff_html_report", entry_point="index_diff.html"
                )
                diff_res.files.extend(_diff_files)
                diff_res.assets.extend(_diff_assets)
        elif _measurement_comparable:
            # Only a comparable run can conclude that nothing coverable changed.
            # When it is not comparable the reason was already printed above, and
            # repeating this sentence would name a cause that was never established.
            print("No C/C++ source files changed — differential coverage report was not generated.")

        # Deliberately NOT an arm of the _diff_ran chain above: _diff_ran is False
        # for three different reasons and only one of them (nothing coverable
        # changed on a comparable run) licenses an OK, so reaching the abstention
        # through it left a green sub-result on a run that did not judge.
        #
        # Guarded on is_ok() as well, because this MUTATES a result rather than
        # constructing one, unlike the two sibling abstentions above: on this path
        # diff_res is what from_commands_run returned for the differential script,
        # so it is already FAIL when that script exited non-zero. The script writes
        # its baseline sidecar and selected-base marker before several later
        # failure paths, so "the tool broke" and "we cannot judge" genuinely
        # co-occur - and a failed REPORT must stay RED, per the contract this job
        # states above. Only a result that is not already a failure may be
        # downgraded; SKIPPED then counts as OK, so the job stays green while
        # stating that it did not judge, and reddening a run that merely could not
        # be compared would turn a tool problem the PR author cannot act on into a
        # blocking failure.
        if not _measurement_comparable and diff_res.is_ok():
            _skip_msg = f"Coverage comparison skipped: {_incomparable_reason}"
            print(_skip_msg)
            diff_res.info = _skip_msg
            diff_res.set_comment(_skip_msg)
            diff_res.set_status(Result.Status.SKIPPED)

        results.append(diff_res)

        # Generate report for changed blocks only
        _print_log = f"{TEMP_DIR}{Utils.normalize_string('Print Uncovered Code')}.log"
        _diff_inputs_exist = (
            Path(TEMP_DIR + "changes.diff").exists()
            and Path(TEMP_DIR + "current.changed.info").exists()
        )
        # Comparability is tested FIRST because the two conditions are not
        # independent: a current-side cause short-circuits the differential script,
        # which is the sole writer of both diff inputs, so their absence there means
        # "the script never ran", not "nothing coverable changed". Testing
        # _diff_inputs_exist first would therefore report the no-C++-changes reason
        # on the very paths this gate exists to abstain on.
        if not _measurement_comparable:
            # The uncovered-code analysis compares the changed-line slice of the two
            # measurements, extracted from the same merged data as the totals, so its
            # output is fabricated for exactly the same reason.
            msg = f"Skipping uncovered code analysis: {_incomparable_reason}"
            print(msg)
            print_res = Result.create_from(
                name="Print Uncovered Code",
                status=Result.Status.SKIPPED,
                info=msg,
            )
            print_res.set_comment(msg)
            _diff_inputs_exist = False
        elif _diff_inputs_exist:
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
        if _diff_ran and _measurement_comparable:
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
            _has_coverage_data = _diff_ran and _measurement_comparable
            if not _has_coverage_data:
                # Comparability is tested FIRST, as at the two sibling sites above,
                # because a current-side cause short-circuits the differential
                # script entirely: _diff_ran is then False for a reason that has
                # nothing to do with C/C++ files, so selecting the message on it
                # states something about the PR's contents that was never
                # established - immediately after the real reason was printed.
                if not _measurement_comparable:
                    print(f"Skipping coverage comment and CI DB row: {_incomparable_reason}")
                else:
                    print("No coverage-relevant changes detected (no C/C++ source changes), skipping coverage comment.")
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
                    # The ancestor the report was actually generated against, which
                    # is generally NOT base_commit_sha: that is the NEAREST master
                    # ancestor, while the selector walks past any that is unusable.
                    # Storing the nearest one would attribute the delta to a
                    # revision that was never measured.
                    "base_commit_sha": _selected_base_commit or base_commit_sha,
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
        if not is_local_run and not _sidecar["complete"]:
            # The post-hook's non-PR branch inserts this row into the coverage CI DB
            # table, which is the series every later baseline and trend reads. An
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
        # Only publish the link when the artifact it addresses exists: the merge
        # phase now legitimately produces no report at all (it exits 0 without
        # generating HTML when merged.profdata is absent), so an unconditional
        # append points the intended green SKIPPED result at a 404. The entry
        # point rather than the directory is tested, because that is what the URL
        # addresses and what collect_html_report_files requires before it attaches
        # anything.
        if Path(f"{TEMP_DIR}/llvm_coverage_html_report/index.html").exists():
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_report/index.html"
            )
        if _diff_ran and _measurement_comparable:
            report_links.append(
                f"{_s3_base}/llvm_coverage/generate_llvm_coverage_diff_report/index_diff.html"
            )

    archives = [
        a
        for a in [
            f"{TEMP_DIR}/llvm_coverage_html_report.tar.gz",
            f"{TEMP_DIR}/llvm_coverage_diff_html_report.tar.gz"
            if (_diff_ran and _measurement_comparable)
            else None,
        ]
        if a and Path(a).exists()
    ]

    Result.create_from(
        results=results,
        files=archives,
        links=report_links,
        info="LLVM Coverage Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
