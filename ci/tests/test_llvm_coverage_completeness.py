"""
Regression tests for the LLVM coverage completeness protocol.

Background
----------
The coverage diff gate compares a baseline coverage percentage against the
current one. Both are merged from per-shard `.profdata` artifacts that are
deliberately `optional=True`, and nothing validated that either side had all of
them. That is wrong in two directions:

* the PR side is short a shard, the total drops, and an unrelated PR is failed
  for a coverage regression it did not cause;
* the BASELINE is short a shard, its total is low, and a real regression passes.

The second direction is the worse one and produces a PASS, so it leaves no trace
in the failure counts at all.

These tests pin the contract: a verdict, and every number derived from it, is
produced only from two complete measurements of the same artifact manifest;
otherwise the job reports SKIPPED with a reason and stays green.

Every row drives the production module, never a copy of its logic.
"""

import ast
import json
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import types
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

# ci.praktika must be imported first: its __init__ puts ci/ on sys.path, which
# ci.defs.defs needs for its bare `from praktika import ...`.
from ci.praktika.job import Job
from ci.praktika.result import Result
from ci.defs.defs import LLVM_ARTIFACTS_LIST, ArtifactConfigs
from ci.jobs.scripts import llvm_coverage_completeness as completeness

_CI_ROOT = os.path.join(os.path.dirname(__file__), "..")
_REPO_ROOT = os.path.join(_CI_ROOT, "..")
_JOB = os.path.join(_CI_ROOT, "jobs", "llvm_coverage_job.py")
_MERGE_SH = os.path.join(_CI_ROOT, "jobs", "scripts", "merge_llvm_coverage.sh")
_SELECT_SH = os.path.join(
    _CI_ROOT, "jobs", "scripts", "generate_diff_coverage_report.sh"
)
_UT_JOB = os.path.join(_CI_ROOT, "jobs", "unit_tests_job.py")
_FT_JOB = os.path.join(_CI_ROOT, "jobs", "functional_tests.py")
_IT_JOB = os.path.join(_CI_ROOT, "jobs", "integration_test_job.py")
_FT_RESULTS = os.path.join(_CI_ROOT, "jobs", "scripts", "functional_tests_results.py")
_CH_TEST = os.path.join(_REPO_ROOT, "tests", "clickhouse-test")
_FILTER_JOB = os.path.join(
    _CI_ROOT, "jobs", "scripts", "workflow_hooks", "filter_job.py"
)
_JOB_CONFIGS = os.path.join(_CI_ROOT, "defs", "job_configs.py")

# A representative manifest. The real one has 21 entries today and has grown
# 18 -> 20 -> 21, which is exactly the drift `manifest_fp` exists to catch.
_NAMES = [f"LLVM_COVERAGE_FILE_shard_{i}" for i in range(1, 22)]
_ALL_PRESENT = [completeness.profile_basename(n) for n in _NAMES]


def _sidecar(names=None, present=None, merge_ok=True, info_path=""):
    return completeness.build_sidecar(
        names if names is not None else _NAMES,
        present if present is not None else _ALL_PRESENT,
        info_path=info_path,
        merge_ok=merge_ok,
    )


# --------------------------------------------------------------------------
# Row 1: both sides complete and matching -> a verdict is produced
# --------------------------------------------------------------------------


def test_two_complete_matching_measurements_are_comparable():
    ok, reason = completeness.comparable(_sidecar(), _sidecar())
    assert ok is True
    assert reason == ""


# --------------------------------------------------------------------------
# Row 2: PR side short -> SKIPPED, and the reason names the missing shard
# --------------------------------------------------------------------------


def test_short_pr_side_is_not_comparable():
    short = _ALL_PRESENT[:-1]
    ok, reason = completeness.comparable(_sidecar(present=short), _sidecar())
    assert ok is False
    assert "PR-side" in reason
    assert _ALL_PRESENT[-1] in reason


# --------------------------------------------------------------------------
# Row 3: BASELINE short while our side is complete -> SKIPPED, not PASS.
# The masked-regression direction; the most important assertion in this file.
# --------------------------------------------------------------------------


def test_short_baseline_is_not_comparable():
    baseline = _sidecar(present=_ALL_PRESENT[:1])  # the measured 1-of-21 shape
    ok, reason = completeness.comparable(_sidecar(), baseline)
    assert ok is False
    assert "baseline" in reason
    assert "incomplete" in reason


def test_short_baseline_would_otherwise_look_like_an_improvement():
    # Why row 3 matters: a 1-of-21 baseline reads far LOWER than a complete
    # current side, so the raw comparison is a coverage *increase* and any real
    # regression inside it passes unnoticed.
    from ci.jobs.llvm_coverage_job import coverage_degraded, coverage_drop

    assert not coverage_degraded(coverage_drop(28.6, 86.4))


# --------------------------------------------------------------------------
# Row 4: manifest drift -> SKIPPED even though both sides are internally complete
# --------------------------------------------------------------------------


def test_manifest_change_is_not_comparable():
    old_names = _NAMES[:18]  # the real 18 -> 21 growth
    baseline = _sidecar(
        names=old_names,
        present=[completeness.profile_basename(n) for n in old_names],
    )
    assert baseline["complete"] is True
    ok, reason = completeness.comparable(_sidecar(), baseline)
    assert ok is False
    assert "manifest" in reason


def test_manifest_fingerprint_is_order_independent_and_content_sensitive():
    assert completeness.manifest_fingerprint(_NAMES) == completeness.manifest_fingerprint(
        list(reversed(_NAMES))
    )
    assert completeness.manifest_fingerprint(_NAMES) != completeness.manifest_fingerprint(
        _NAMES[:-1]
    )


# --------------------------------------------------------------------------
# Row 5: no baseline sidecar at all -> SKIPPED, never an exception.
# Back-compat: no master commit published before this change has one.
# --------------------------------------------------------------------------


def test_missing_baseline_sidecar_is_not_comparable_and_does_not_raise():
    ok, reason = completeness.comparable(_sidecar(), None)
    assert ok is False
    assert "no completeness metadata" in reason


def test_reading_an_absent_sidecar_returns_none():
    with tempfile.TemporaryDirectory() as d:
        assert completeness.read_sidecar(os.path.join(d, "nope.json")) is None


def test_reading_a_corrupt_sidecar_returns_none_instead_of_raising():
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "llvm_coverage.meta.json")
        with open(p, "w") as f:
            f.write("{not json")
        assert completeness.read_sidecar(p) is None


# --------------------------------------------------------------------------
# Row 6: unknown/newer schema version -> SKIPPED, not an exception
# --------------------------------------------------------------------------


def test_newer_baseline_schema_version_is_not_comparable():
    baseline = _sidecar()
    baseline["schema_version"] = completeness.SCHEMA_VERSION + 1
    ok, reason = completeness.comparable(_sidecar(), baseline)
    assert ok is False
    assert "schema version" in reason


# --------------------------------------------------------------------------
# Row 7: an EXTRA/foreign profile makes the measurement INCOMPLETE.
# Passes only under set EQUALITY; a set difference accepts extras.
# --------------------------------------------------------------------------


def test_extra_foreign_profile_makes_the_measurement_incomplete():
    with_extra = _ALL_PRESENT + ["merged.profdata"]
    s = _sidecar(present=with_extra)
    assert s["complete"] is False
    assert s["missing"] == []
    assert s["unexpected"] == ["merged.profdata"]
    ok, reason = completeness.comparable(s, _sidecar())
    assert ok is False
    assert "unexpected" in reason


def test_a_set_difference_would_accept_the_extra():
    # Pins WHY equality is required: `expected - present` is empty here, so a
    # difference-based predicate would call this complete.
    expected = set(_ALL_PRESENT)
    present = set(_ALL_PRESENT + ["merged.profdata"])
    assert expected - present == set()
    assert expected != present


# --------------------------------------------------------------------------
# Row 7b: the merge is given an explicit list, never a glob
# --------------------------------------------------------------------------


def test_merge_inputs_are_manifest_derived_and_exclude_foreign_files():
    inputs = completeness.merge_inputs(
        _NAMES, _ALL_PRESENT + ["merged.profdata", "stale-name.profdata"]
    )
    assert inputs == sorted(_ALL_PRESENT)
    assert "merged.profdata" not in inputs
    assert "stale-name.profdata" not in inputs


def test_merge_script_refuses_an_unbounded_glob():
    src = open(_MERGE_SH, encoding="utf-8").read()
    assert "MERGE_PROFDATA_FILES" in src
    assert "refusing to merge an unbounded glob" in src
    # The old unrestricted glob must be gone from the merge invocation.
    assert "-failure-mode=any" in src
    assert "*.profdata -o merged.profdata" not in src


def test_job_passes_the_intersection_to_the_merge_step():
    src = open(_JOB, encoding="utf-8").read()
    assert "completeness.merge_inputs(" in src
    assert "MERGE_PROFDATA_FILES=" in src


# --------------------------------------------------------------------------
# Row 7c: a legitimately absent optional shard still merges the rest
# --------------------------------------------------------------------------


def test_twenty_of_twenty_one_still_merges_those_twenty_but_skips_the_verdict():
    present = _ALL_PRESENT[:-1]
    inputs = completeness.merge_inputs(_NAMES, present)
    assert len(inputs) == 20  # the merge is not starved
    ok, _ = completeness.comparable(_sidecar(present=present), _sidecar())
    assert ok is False  # but no verdict is produced


# --------------------------------------------------------------------------
# Row 8: a producer with no profile at all is "absent", not "complete with zero"
# --------------------------------------------------------------------------


def test_no_profiles_at_all_is_incomplete_not_complete_with_zero():
    s = _sidecar(present=[])
    assert s["complete"] is False
    assert len(s["missing"]) == len(_NAMES)


def test_present_profiles_snapshot_ignores_non_profdata_files():
    with tempfile.TemporaryDirectory() as d:
        for name in ("a.profdata", "b.profdata", "notes.txt", "clickhouse"):
            with open(os.path.join(d, name), "w") as f:
                f.write("x")
        os.mkdir(os.path.join(d, "dir.profdata"))
        assert completeness.present_profiles(d) == ["a.profdata", "b.profdata"]


# --------------------------------------------------------------------------
# Rows 8b / 8c / 8d / 15: a shard that did not complete publishes NO profile,
# while a completed run WITH failing tests still publishes its profile.
# --------------------------------------------------------------------------


def _ft_completion(runner_output: str) -> bool:
    """Drive the real FT parser over a runner transcript."""
    sys.path.insert(0, _CI_ROOT)
    from ci.jobs.scripts.functional_tests_results import FTResultsProcessor

    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "test_result.txt"), "w") as f:
            f.write(runner_output)
        return FTResultsProcessor(wd=d)._process_test_output().coverage_run_complete


def _ft_success_finish(runner_output: str) -> bool:
    sys.path.insert(0, _CI_ROOT)
    from ci.jobs.scripts.functional_tests_results import FTResultsProcessor

    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "test_result.txt"), "w") as f:
            f.write(runner_output)
        return FTResultsProcessor(wd=d)._process_test_output().success_finish


_OK_LINE = "00001_test: [ OK ] 0.10 sec.\n"
_FAIL_LINE = "00002_test: [ FAIL ] 0.10 sec.\n"
_COMPLETE = "Coverage run completed all selected tests.\n"


def test_completed_run_with_failing_tests_still_publishes_its_profile():
    # 8c: the discriminating row. A gate keyed on "no test failed" would pass
    # every other cell here while silently DELETING coverage on the common path.
    out = _OK_LINE + _FAIL_LINE + _COMPLETE + "All tests have finished.\n"
    assert _ft_completion(out) is True


def test_zero_test_run_publishes_no_profile():
    # 8d: `No tests were run.` sets success_finish True and exits 1, and the
    # instrumented server still wrote .profraw files, so a predicate keyed on
    # success_finish would publish a startup-only profile.
    out = "No tests were run.\n"
    assert _ft_completion(out) is False
    assert _ft_success_finish(out) is True


def test_run_that_died_mid_way_publishes_no_profile():
    out = _OK_LINE + _OK_LINE
    assert _ft_completion(out) is False
    assert _ft_success_finish(out) is False


def test_killed_worker_run_publishes_no_profile_even_though_the_legacy_marker_printed():
    # 15: the r19 regression test. A worker that dies abnormally only sets
    # runner_process_killed, while the counter tested at the end is the SELECTED
    # test count, so `All tests have finished.` is printed anyway.
    out = _OK_LINE + "All tests have finished.\n"
    assert _ft_success_finish(out) is True
    assert _ft_completion(out) is False


def test_success_finish_semantics_are_unchanged():
    # The new field is additive: success_finish keeps its own contract, which
    # three unrelated call sites depend on.
    from ci.jobs.scripts.functional_tests_results import SUCCESS_FINISH_SIGNS

    assert SUCCESS_FINISH_SIGNS == ["All tests have finished", "No tests were run"]


_MARKER = "Coverage run completed all selected tests."


def _marker_predicate_node():
    """The real `if` statement in tests/clickhouse-test that prints the marker.

    Selected as the INNERMOST enclosing ast.If by smallest line span:
    ast.walk yields outer nodes first, so its first match could be an enclosing
    `if` whose span covers most of main.
    """
    src = open(_CH_TEST, encoding="utf-8").read()
    tree = ast.parse(src)
    enclosing = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If) and _MARKER in (ast.get_source_segment(src, n) or "")
    ]
    assert enclosing, "no if statement encloses the coverage marker"
    return min(enclosing, key=lambda n: n.end_lineno - n.lineno)


def test_runner_emits_the_marker_only_when_it_completed_and_kept_its_workers():
    # EXECUTES the real predicate rather than asserting that both operand
    # substrings appear somewhere near it: replaying two such `in` checks against
    # a window whose `and` has been swapped for `or` passes, so a substring pair
    # is blind to the operator by construction - and `or` admits exactly the two
    # states this marker exists to exclude (a killed worker and a zero-test run),
    # each of which then lets a short profile be merged.
    src = open(_CH_TEST, encoding="utf-8").read()
    assert _MARKER in src
    node = _marker_predicate_node()

    # Exact, not two independent `in` checks: an operator swap must fail
    # structurally as well as behaviourally.
    assert (
        ast.unparse(node.test)
        == "total_tests_run != 0 and (not runner_process_killed.is_set())"
    ), ast.unparse(node.test)

    mod = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(mod)
    code = compile(mod, _CH_TEST, "exec")

    class _Event:
        def __init__(self, state):
            self._state = state

        def is_set(self):
            return self._state

    emitted = {}
    for total_tests_run in (0, 7):
        for killed in (False, True):
            out = []
            exec(
                code,
                {
                    "total_tests_run": total_tests_run,
                    "runner_process_killed": _Event(killed),
                    "print": lambda *a, **k: out.append(" ".join(str(x) for x in a)),
                },
            )
            emitted[(total_tests_run, killed)] = any(_MARKER in line for line in out)

    assert emitted == {
        (7, False): True,  # completed and kept its workers: the only publishing state
        (7, True): False,  # the r19 case: a worker was killed mid-run
        (0, False): False,  # zero tests ran, so nothing was measured
        (0, True): False,
    }, emitted

    # The existing marker text and exit contract stay untouched.
    assert 'print("All tests have finished.")' in src
    assert 'print("No tests were run.")' in src


def test_ft_job_gates_profile_creation_not_attachment():
    # The r5 no-op: skipping `R.files.append` leaves the file on disk, where the
    # runner's own glob still uploads it. The gate must sit on the merge.
    src = open(_FT_JOB, encoding="utf-8").read()
    assert 'coverage_run_complete' in src
    gate = src.index('_coverage_run_complete = bool(')
    merge = src.index('merge -sparse -failure-mode=any')
    append = src.index('R.files.append(merged_file)')
    assert gate < merge < append


def test_integration_job_snapshots_completion_before_the_coverage_block():
    src = open(_IT_JOB, encoding="utf-8").read()
    snapshot = src.index("coverage_run_complete = coverage_phases_complete")
    coverage_block = src.index("    if is_llvm_coverage:\n        assert (")
    merge_call = src.index("merged_profdata = merge_profraw_files(")
    # The coverage block resets has_error, so a later read answers a different
    # question; and the merge site must consult the snapshot, not re-derive it.
    assert snapshot < coverage_block < merge_call
    assert "if not coverage_run_complete:" in src


def test_integration_job_records_the_silent_phase_drop():
    # Route 5a: the MAX_FAILS_BEFORE_DROP skip sets no termination status of its
    # own, so it must be recorded where the decision is made.
    src = open(_IT_JOB, encoding="utf-8").read()
    assert "coverage_phases_complete = False" in src
    assert "MAX_FAILS_BEFORE_DROP and not has_error" in src


# --------------------------------------------------------------------------
# Rows 9b / route 6: a zero-length raw makes the shard incomplete, and the
# premise (the tool ignores it silently) is asserted by measurement.
# --------------------------------------------------------------------------


def _find_tool(base):
    """First available versioned spelling of an LLVM tool, mirroring the jobs."""
    for ver in ["21", "20", "19", "18", "17", "16", ""]:
        cmd = f"{base}{'-' + ver if ver else ''}"
        found = shutil.which(cmd)
        if found:
            return found
    return None


def test_zero_length_raw_is_silently_ignored_by_the_tool():
    # The PREMISE behind the explicit zero-length filter: --failure-mode=any does
    # NOT cover this, so the producers must reject it themselves.
    tool = _find_tool("llvm-profdata")
    clang = _find_tool("clang")
    if not tool or not clang:
        return  # no LLVM toolchain in this environment
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "t.c")
        with open(src, "w") as f:
            f.write("int main(){return 0;}\n")
        exe = os.path.join(d, "t")
        if subprocess.run(
            [clang, "-fprofile-instr-generate", "-fcoverage-mapping", src, "-o", exe],
            capture_output=True,
        ).returncode != 0:
            return
        good = os.path.join(d, "good.profraw")
        env = dict(os.environ, LLVM_PROFILE_FILE=good)
        subprocess.run([exe], env=env, capture_output=True)
        if not os.path.exists(good):
            return
        empty = os.path.join(d, "empty.profraw")
        open(empty, "w").close()

        only_good = os.path.join(d, "a.profdata")
        with_empty = os.path.join(d, "b.profdata")
        r1 = subprocess.run(
            [tool, "merge", "-sparse", "-failure-mode=any", good, "-o", only_good],
            capture_output=True,
        )
        r2 = subprocess.run(
            [tool, "merge", "-sparse", "-failure-mode=any", good, empty, "-o", with_empty],
            capture_output=True,
        )
        assert r1.returncode == 0
        # The tool accepts the empty input and produces the same merge: no signal.
        assert r2.returncode == 0
        assert open(only_good, "rb").read() == open(with_empty, "rb").read()


def test_every_producer_rejects_zero_length_raw_inputs():
    for path in (_UT_JOB, _FT_JOB, _IT_JOB):
        src = open(path, encoding="utf-8").read()
        assert "getsize(f) == 0" in src, path
        assert "are empty" in src, path


def test_every_producer_merge_is_all_or_nothing():
    for path in (_UT_JOB, _FT_JOB, _IT_JOB):
        src = open(path, encoding="utf-8").read()
        assert "-failure-mode=any" in src, path
        assert "-failure-mode=warn" not in src, path


def test_producers_no_longer_parse_corrupt_strings():
    # Obsolete once the merge is all-or-nothing: a corrupt input produces no
    # file, so there is nothing to classify by message text.
    for path in (_UT_JOB, _FT_JOB, _IT_JOB):
        src = open(path, encoding="utf-8").read()
        assert "invalid instrumentation profile" not in src, path
        assert "file header is corrupt" not in src, path


# --------------------------------------------------------------------------
# Row 9c: the UT producer needs no completion gate - the runner already
# prevents the upload. Asserted structurally.
# --------------------------------------------------------------------------


def test_unit_test_job_relies_on_the_runner_to_withhold_a_failed_shards_profile():
    src = open(_UT_JOB, encoding="utf-8").read()
    # No do_not_block_pipeline_on_failure, so a non-OK result exits 1 ...
    assert "R.complete_job()" in src
    assert "do_not_block_pipeline_on_failure" not in src
    runner = open(
        os.path.join(_CI_ROOT, "praktika", "runner.py"), encoding="utf-8"
    ).read()
    # ... and the runner then skips artifact upload entirely.
    assert "run_exit_code == 0 or result.do_not_block_pipeline_on_failure()" in runner


# --------------------------------------------------------------------------
# Rows 10b / 10c / 10d: the fabricated numbers are withheld on every surface
# --------------------------------------------------------------------------


def test_no_coverage_comment_or_cidb_row_when_incomparable():
    src = open(_JOB, encoding="utf-8").read()
    assert "_has_coverage_data = _diff_ran and _measurement_comparable" in src


def test_master_path_writes_no_cidb_row_for_an_incomplete_measurement():
    # The post-hook's non-PR branch still inserts, so gating only the comment
    # would keep poisoning the baseline series every later PR compares against.
    src = open(_JOB, encoding="utf-8").read()
    assert 'if not is_local_run and not _sidecar["complete"]:' in src


def test_uncovered_code_analysis_is_skipped_when_incomparable():
    # Comparability is tested FIRST, unconjoined: on a current-side cause the
    # differential script never runs, so its two output files are absent and a
    # guard conjoined with _diff_inputs_exist could not fire at all. The
    # behavioural counterparts are the three
    # test_incomparable_*_uncovered_analysis / test_a_comparable_run_with_no_*
    # cells below, which observe print_res rather than the source text.
    src = open(_JOB, encoding="utf-8").read()
    assert "if not _measurement_comparable:" in src
    assert "if _diff_inputs_exist and not _measurement_comparable:" not in src
    # ... and its counts block is withheld too.
    assert "if _diff_ran and _measurement_comparable:" in src


def test_differential_report_is_withheld_but_the_full_report_is_only_labelled():
    src = open(_JOB, encoding="utf-8").read()
    # differential: attachment and link both gated
    assert "llvm_coverage_diff_html_report.tar.gz" in src
    assert "if _diff_ran and _measurement_comparable:" in src
    # full report: still attached, with an explicit partial banner. It is the
    # best artifact for finding out WHICH shard went missing.
    assert "partial measurement:" in src


def test_baseline_info_digest_detects_a_torn_pair():
    with tempfile.TemporaryDirectory() as d:
        info = os.path.join(d, "base_llvm_coverage.info")
        with open(info, "w") as f:
            f.write("SF:/a.cpp\nDA:1,1\nend_of_record\n")
        baseline = _sidecar(info_path=info)
        assert baseline["info_digest"]
        # Same-SHA rerun replaced the .info but not the sidecar.
        with open(info, "w") as f:
            f.write("SF:/a.cpp\nDA:1,0\nend_of_record\n")
        ok, reason = completeness.comparable(
            _sidecar(), baseline, baseline_info_path=info
        )
        assert ok is False
        assert "does not describe" in reason


def test_absent_info_digest_does_not_block_a_complete_baseline():
    baseline = _sidecar()  # no info_path -> digest ""
    ok, _ = completeness.comparable(_sidecar(), baseline, baseline_info_path="")
    assert ok is True


# --------------------------------------------------------------------------
# Rows 12 / 13: an aggregate MERGE failure is SKIPPED and green; a genuine
# REPORT failure stays RED. One cell cannot distinguish them.
# --------------------------------------------------------------------------


def test_failed_aggregate_merge_is_not_comparable():
    ok, reason = completeness.comparable(_sidecar(merge_ok=False), _sidecar())
    assert ok is False
    assert "merge failed" in reason


def test_merge_and_report_are_separate_job_steps():
    src = open(_JOB, encoding="utf-8").read()
    assert 'name="Merge LLVM Coverage Profiles"' in src
    assert "merge_llvm_coverage.sh merge" in src
    assert "merge_llvm_coverage.sh report" in src
    # The merge status is read from a marker, because a step's exit code is
    # collapsed to a boolean and could not distinguish the two failures.
    assert "merge_profdata.status" in src


def _run_merge_phase(profdata_ok: bool, files="a.profdata"):
    """Drive the real merge phase against a synthetic llvm-profdata.

    Returns (exit_code, marker_contents, info_exists). A shim is used because the
    property under test is how the script REACTS to a failing merge, not what
    llvm-profdata does (that is measured separately).
    """
    with tempfile.TemporaryDirectory() as d:
        bin_dir = os.path.join(d, "bin")
        os.makedirs(bin_dir)
        shim = os.path.join(bin_dir, "llvm-profdata")
        with open(shim, "w") as f:
            if profdata_ok:
                # Honour -o like the real tool does.
                f.write(
                    "#!/bin/bash\nwhile [ $# -gt 0 ]; do"
                    ' if [ "$1" = "-o" ]; then echo data > "$2"; fi; shift; done\nexit 0\n'
                )
            else:
                f.write('#!/bin/bash\necho "error: no profile can be merged" >&2\nexit 1\n')
        os.chmod(shim, 0o755)
        with open(os.path.join(bin_dir, "llvm-cov"), "w") as f:
            f.write("#!/bin/bash\nexit 0\n")
        os.chmod(os.path.join(bin_dir, "llvm-cov"), 0o755)

        work = os.path.join(d, "work")
        os.makedirs(os.path.join(work, "ci", "tmp"))
        for name in files.split():
            open(os.path.join(work, "ci", "tmp", name), "w").close()
        shutil.copy2(_MERGE_SH, os.path.join(work, "merge.sh"))

        env = dict(
            os.environ,
            PATH=bin_dir + os.pathsep + os.environ["PATH"],
            MERGE_PROFDATA_FILES=files,
            LLVM_PROFDATA=shim,
            LLVM_COV=os.path.join(bin_dir, "llvm-cov"),
        )
        r = subprocess.run(
            ["bash", "merge.sh", "merge"], cwd=work, env=env, capture_output=True, text=True
        )
        marker = os.path.join(work, "ci", "tmp", "merge_profdata.status")
        return (
            r.returncode,
            open(marker).read().strip() if os.path.exists(marker) else None,
            os.path.exists(os.path.join(work, "ci", "tmp", "llvm_coverage.info")),
        )


def test_merge_phase_reports_failure_without_reddening_and_without_faking_an_info():
    # 12: a failed aggregate merge must leave the job GREEN with an explicit
    # reason, not RED, and must fabricate no .info to get there.
    rc, marker, info_exists = _run_merge_phase(profdata_ok=False)
    assert rc == 0, "a merge failure must not redden the step"
    assert marker == "failed"
    assert info_exists is False


def test_merge_phase_reports_success_through_the_same_marker():
    rc, marker, _ = _run_merge_phase(profdata_ok=True)
    assert rc == 0
    assert marker == "ok"


def test_merge_phase_refuses_an_unset_file_list():
    # Without the explicit list the script would fall back to a glob, which is
    # route 3; it must refuse instead.
    with tempfile.TemporaryDirectory() as d:
        work = os.path.join(d, "work")
        os.makedirs(os.path.join(work, "ci", "tmp"))
        shutil.copy2(_MERGE_SH, os.path.join(work, "merge.sh"))
        env = {k: v for k, v in os.environ.items() if k != "MERGE_PROFDATA_FILES"}
        env["LLVM_PROFDATA"] = "/bin/true"
        env["LLVM_COV"] = "/bin/true"
        r = subprocess.run(
            ["bash", "merge.sh", "merge"], cwd=work, env=env, capture_output=True, text=True
        )
        assert r.returncode != 0
        assert "unbounded glob" in r.stdout + r.stderr


def test_report_phase_still_fails_hard():
    # 13: the counterpart to row 12. A genuine report-generation failure is a
    # tooling failure and must stay RED; one cell cannot show both.
    with tempfile.TemporaryDirectory() as d:
        bin_dir = os.path.join(d, "bin")
        os.makedirs(bin_dir)
        for name, body in (
            ("llvm-profdata", "#!/bin/bash\nexit 0\n"),
            # llvm-cov export fails: this is NOT an incompleteness signal.
            ("llvm-cov", '#!/bin/bash\nif [ "$1" = "export" ]; then exit 3; fi\nexit 0\n'),
        ):
            p_ = os.path.join(bin_dir, name)
            with open(p_, "w") as f:
                f.write(body)
            os.chmod(p_, 0o755)
        work = os.path.join(d, "work")
        os.makedirs(os.path.join(work, "ci", "tmp"))
        # merged.profdata present, so the report phase proceeds past its guard.
        open(os.path.join(work, "ci", "tmp", "merged.profdata"), "w").close()
        open(os.path.join(work, "ci", "tmp", "clickhouse"), "w").close()
        shutil.copy2(_MERGE_SH, os.path.join(work, "merge.sh"))
        env = dict(
            os.environ,
            PATH=bin_dir + os.pathsep + os.environ["PATH"],
            LLVM_PROFDATA=os.path.join(bin_dir, "llvm-profdata"),
            LLVM_COV=os.path.join(bin_dir, "llvm-cov"),
            WORKSPACE_PATH=work,
        )
        r = subprocess.run(
            ["bash", "merge.sh", "report"], cwd=work, env=env, capture_output=True, text=True
        )
        assert r.returncode != 0, "a report-generation failure must stay RED"


def test_report_phase_does_nothing_when_the_merge_produced_no_profile():
    # The merge phase legitimately leaves no merged.profdata; the report phase must
    # then exit cleanly rather than throwing on the absent input.
    with tempfile.TemporaryDirectory() as d:
        work = os.path.join(d, "work")
        os.makedirs(os.path.join(work, "ci", "tmp"))
        shutil.copy2(_MERGE_SH, os.path.join(work, "merge.sh"))
        env = dict(os.environ, LLVM_PROFDATA="/bin/true", LLVM_COV="/bin/true")
        r = subprocess.run(
            ["bash", "merge.sh", "report"], cwd=work, env=env, capture_output=True, text=True
        )
        assert r.returncode == 0
        assert "nothing to report on" in r.stdout


def _drive_merge_gate(present_profiles, tmpdir, artifact_names=None, marker=None):
    """Drive the real merge-invocation decision out of the job source.

    Returns a namespace with the merge-script invocations, merge_res and _merge_ok.
    The real statements are executed in their real order; only from_commands_run is
    stubbed, and that stub reproduces the script's observable behaviour (it writes
    the status marker) so the marker read is exercised rather than bypassed.
    """
    src = open(_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    keep = []
    started = False
    for st in main_if.body:
        txt = ast.get_source_segment(src, st) or ""
        if txt.startswith("_expected_artifacts = "):
            started = True
        if not started:
            continue
        keep.append(st)
        if txt.startswith("if not _merge_ok:"):
            break
    mod = ast.Module(body=keep, type_ignores=[])
    ast.fix_missing_locations(mod)
    code = compile(mod, _JOB, "exec")

    for profile in present_profiles:
        with open(os.path.join(tmpdir, profile), "wb") as f:
            f.write(b"\x00profdata\x00")

    invocations = []

    def _from_commands_run(name, command, **kwargs):
        invocations.append(command)
        if marker is not None:
            with open(os.path.join(tmpdir, "merge_profdata.status"), "w") as f:
                f.write(marker)
        return Result.create_from(name=name, status=True)

    class _ResultShim:
        Status = Result.Status
        create_from = staticmethod(Result.create_from)
        from_commands_run = staticmethod(_from_commands_run)

    ns = {
        "Path": pathlib.Path,
        "TEMP_DIR": tmpdir,
        "Result": _ResultShim,
        "completeness": completeness,
        "shlex": shlex,
        "print": lambda *a, **k: None,
        "LLVM_ARTIFACTS_LIST": list(
            _GATE_ARTIFACTS if artifact_names is None else artifact_names
        ),
        "results": [],
    }
    exec(code, ns)
    return types.SimpleNamespace(
        invocations=invocations,
        merge_res=ns.get("merge_res"),
        merge_ok=ns.get("_merge_ok"),
        results=ns["results"],
        sidecar_inputs=ns.get("_merge_inputs"),
    )


def test_zero_arriving_profiles_reports_skipped_instead_of_reddening():
    # Every shard profile is optional=True, so "none arrived" is the limit of the
    # case the job promises to report as a green SKIPPED. Invoking the merge script
    # with an empty file list instead exits 1 on its unbounded-glob guard, which
    # cannot distinguish an empty value from an omitted one, so the FAIL child
    # reddens the whole job BEFORE the marker mechanism the SKIPPED verdict is
    # built on is ever reached.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_merge_gate([], d)
    assert got.invocations == [], f"merge script was invoked: {got.invocations}"
    assert got.merge_res.status == Result.Status.SKIPPED, got.merge_res.status
    assert got.merge_ok is False
    # The assertion that pins the invariant: the composite job result stays green.
    parent = Result.create_from(name="LLVM Coverage", results=[got.merge_res])
    assert parent.is_ok(), f"job is not green: {parent.status}"
    # And the sidecar must record the measurement as unusable, not as complete.
    sidecar = completeness.build_sidecar(
        _GATE_ARTIFACTS, [], info_path="", merge_ok=got.merge_ok
    )
    assert sidecar["complete"] is False


def test_at_least_one_arriving_profile_still_invokes_the_merge_script():
    # Reverse direction. Without this the new branch could be widened to always
    # skip and the aggregate merge would silently stop running.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_merge_gate(["COV_A.profdata"], d, marker="ok")
    assert len(got.invocations) == 1, f"expected one invocation, got {got.invocations}"
    joined = " ".join(got.invocations[0])
    assert "merge_llvm_coverage.sh merge" in joined
    assert "MERGE_PROFDATA_FILES=COV_A.profdata" in joined, joined
    assert got.merge_ok is True


def test_the_scripts_are_syntactically_valid():
    for path in (_MERGE_SH, _SELECT_SH):
        assert (
            subprocess.run(["bash", "-n", path], capture_output=True).returncode == 0
        ), path


# --------------------------------------------------------------------------
# Row 14: a missing llvm_coverage.info must not turn the JOB into ERROR
# --------------------------------------------------------------------------


def test_coverage_info_artifact_is_optional_so_a_skipped_verdict_survives():
    # runner.py re-checks artifact paths AFTER the job exits and raises
    # FileNotFoundError -> ERROR for a missing non-optional path, which would
    # override the job's own SKIPPED.
    assert ArtifactConfigs.llvm_coverage_info_file.optional is True


def test_the_sidecar_rides_on_the_existing_artifact_so_no_new_name_is_introduced():
    paths = ArtifactConfigs.llvm_coverage_info_file.path
    assert isinstance(paths, list)
    assert any(p.endswith("llvm_coverage.info") for p in paths)
    assert any(p.endswith(completeness.SIDECAR_BASENAME) for p in paths)


_GATE_ARTIFACTS = ["COV_A", "COV_B"]


def _load_job_module_namespace():
    """The job's module-level definitions, without running its __main__ block."""
    src = open(_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    body = [n for n in tree.body if not isinstance(n, ast.If)]
    mod = ast.Module(body=body, type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = {"__name__": "_job_defs"}
    exec(compile(mod, _JOB, "exec"), ns)
    return ns


def _write_baseline_outputs(tmpdir, artifact_names, complete=True, diff_inputs=True):
    """Reproduce what generate_diff_coverage_report.sh leaves in TEMP_DIR.

    The script is the SOLE writer of base_llvm_coverage.meta.json (its wget) and
    of selected_base_commit.txt (its echo), so a stub that writes neither makes
    the whole baseline side unobservable and the harness blind to the order of
    the reads that consume them. Every field is produced by the real module
    functions rather than a literal dict, so a schema change cannot leave this
    stub silently describing something the production reader no longer accepts.

    diff_inputs=False reproduces the script's `${#patterns[@]} -eq 0` exit: it
    has already fetched and selected the baseline, but nothing coverable changed,
    so it never extracts current.changed.info and never runs genhtml - hence no
    report directory either. That is the only state in which the job may
    legitimately conclude "No C/C++ source files changed", so it is the reverse
    direction of the comparability-first ordering below.
    """
    base_info = os.path.join(tmpdir, "base_llvm_coverage.info")
    with open(base_info, "w", encoding="utf-8") as f:
        f.write("TN:\nSF:/src/a.cpp\nDA:1,1\nend_of_record\n")
    present = [completeness.profile_basename(n) for n in artifact_names]
    if not complete:
        present = present[:-1]
    completeness.write_sidecar(
        os.path.join(tmpdir, "base_llvm_coverage.meta.json"),
        completeness.build_sidecar(artifact_names, present, info_path=base_info),
    )
    with open(os.path.join(tmpdir, "selected_base_commit.txt"), "w", encoding="utf-8") as f:
        f.write("a" * 40 + "\n")
    if not diff_inputs:
        return
    os.makedirs(os.path.join(tmpdir, "llvm_coverage_diff_html_report"), exist_ok=True)
    # genhtml's --diff-file and print_uncovered_code.py's two inputs.
    with open(os.path.join(tmpdir, "changes.diff"), "w", encoding="utf-8") as f:
        f.write("diff --git a/src/a.cpp b/src/a.cpp\n")
    with open(os.path.join(tmpdir, "current.changed.info"), "w", encoding="utf-8") as f:
        f.write("SF:/src/a.cpp\nDA:1,1\nend_of_record\n")


def _drive_diff_gate(
    info_present,
    tmpdir,
    job_path=None,
    artifact_names=None,
    present_names=None,
    merge_ok=True,
    baseline_complete=True,
    sidecar_override=None,
    diff_inputs=True,
    pr_number=4242,
    script_fails=False,
):
    """Drive the real diff-gate out of the job source.

    Returns a namespace with the script invocations, diff_res, the comparability
    verdict and reason, the selected base commit and every printed line.

    The statement ORDER is a property under test: the baseline sidecar and the
    selected-base marker are written BY the differential script, so a read placed
    above it can only ever see nothing. The real statements therefore run in
    their real order; only from_commands_run is stubbed, and that stub both
    reproduces the script's precondition (non-zero when llvm_coverage.info is
    absent, measured separately) and writes the files the real script writes.
    """
    src = open(job_path or _JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    block = [
        s
        for s in main_if.body
        if isinstance(s, ast.If) and ast.unparse(s.test) == "not is_master_branch"
    ][0]
    # Slice through the `if not is_local_run:` block, not merely to the
    # comparability verdict: the "Baseline coverage / Current coverage / Delta"
    # prints, the whole Print Uncovered Code branch AND the coverage-comment /
    # CI-DB-row branch all live below it, and a cell asserting on any of them is
    # VACUOUS unless they are in the executed region at all.
    keep = []
    for st in block.body:
        keep.append(st)
        txt = ast.get_source_segment(src, st) or ""
        if txt.startswith("if not is_local_run:"):
            break
    _sliced = "\n".join((ast.get_source_segment(src, s) or "") for s in keep)
    assert (
        "Baseline coverage" in _sliced
    ), "harness slice no longer contains the delta prints it asserts on"
    assert (
        "No C/C++ source files changed" in _sliced
    ), "harness slice no longer contains the no-C++-changes messages it asserts on"
    assert (
        "No coverage-relevant changes detected" in _sliced
    ), "harness slice no longer contains the coverage-comment branch it asserts on"
    mod = ast.Module(body=keep, type_ignores=[])
    ast.fix_missing_locations(mod)
    code = compile(mod, job_path or _JOB, "exec")

    names = list(_GATE_ARTIFACTS if artifact_names is None else artifact_names)
    present = (
        [completeness.profile_basename(n) for n in names]
        if present_names is None
        else list(present_names)
    )
    for profile in present:
        with open(os.path.join(tmpdir, profile), "wb") as f:
            f.write(b"\x00profdata\x00")

    info_path = os.path.join(tmpdir, "llvm_coverage.info")
    if info_present:
        with open(info_path, "w", encoding="utf-8") as f:
            f.write("TN:\nSF:/src/a.cpp\nDA:1,1\nend_of_record\n")

    invocations = []

    def _from_commands_run(name, command, **kwargs):
        invocations.append(command)
        ok = os.path.exists(info_path)
        if ok:
            _write_baseline_outputs(
                tmpdir, names, complete=baseline_complete, diff_inputs=diff_inputs
            )
        # script_fails reproduces a LATER failure of the differential script: it
        # has already written the baseline sidecar and the selected-base marker
        # (its wget at :64 and its echo at :100) and only then hits `exit 1` on an
        # empty GitHub compare, or a set -euo pipefail failure of a gh api call,
        # of either lcov --extract, or of genhtml. Writing those outputs FIRST is
        # exactly what makes the mixed "the tool broke AND we cannot judge" state
        # real, so the write above is deliberately not skipped. The existing
        # os.path.exists(info_path) term is kept so the absent-.info precondition
        # cell is unaffected.
        if script_fails:
            ok = False
        return Result.create_from(name=name, status=ok)

    class _ResultShim:
        Status = Result.Status
        create_from = staticmethod(Result.create_from)
        from_commands_run = staticmethod(_from_commands_run)
        from_fs = staticmethod(
            lambda name: Result.create_from(name=name, status=Result.Status.OK)
        )

    printed = []
    sidecar = (
        completeness.build_sidecar(
            names, present, info_path=info_path, merge_ok=merge_ok
        )
        if sidecar_override is None
        else sidecar_override
    )
    # lcov is not required to be installed here, so the only summary values the
    # slice needs are stubbed - but the two verdict helpers are the REAL ones, so
    # the tolerance semantics under test cannot drift from production. The two
    # sides return DISTINCT percentages keyed on the path, so the printed numbers
    # are distinguishable and a swapped-argument regression is visible; the 0.30
    # pp gap is an improvement, which keeps the healthy arm's verdict green.
    def _stub_lcov_summary(path):
        pct = 84.10 if "base_" in os.path.basename(path) else 84.40
        return ((pct, 1, 2), (pct, 1, 2), (pct, 1, 2))

    job_ns = _load_job_module_namespace()
    ns = {
        "Path": pathlib.Path,
        "TEMP_DIR": tmpdir,
        "Result": _ResultShim,
        "Shell": types.SimpleNamespace(run=lambda *a, **k: None),
        "Utils": types.SimpleNamespace(
            compress_gz=lambda *a, **k: None,
            normalize_string=lambda s: s.lower().replace(" ", "_"),
        ),
        "completeness": completeness,
        "print": lambda *a, **k: printed.append(" ".join(str(x) for x in a)),
        "shutil": shutil,
        "get_lcov_summary": _stub_lcov_summary,
        "coverage_drop": job_ns["coverage_drop"],
        "coverage_degraded": job_ns["coverage_degraded"],
        "collect_html_report_files": lambda *a, **k: ([], []),
        "COVERAGE_DROP_TOLERANCE": job_ns["COVERAGE_DROP_TOLERANCE"],
        "_sidecar": sidecar,
        "_measurement_comparable": True,
        "_incomparable_reason": "",
        "results": [],
        # The coverage-comment / CI-DB-row branch. is_local_run is False so the
        # branch actually EXECUTES: on a local run the job prints one line and
        # writes nothing, which would make every cell below it vacuous.
        "is_local_run": False,
        "pr_number": pr_number,
        "current_commit_sha": "c" * 40,
        "branch": "some-branch",
        "base_commit_sha": "b" * 40,
        "base_branch": "master",
        "datetime": datetime,
        "timezone": timezone,
        "json": json,
        "open": open,
        "S3_REPORT_BUCKET_HTTP_ENDPOINT": "s3.example.invalid",
    }
    exec(code, ns)
    return types.SimpleNamespace(
        invocations=invocations,
        diff_res=ns.get("diff_res"),
        print_res=ns.get("print_res"),
        results=ns.get("results"),
        comparable=ns.get("_measurement_comparable"),
        reason=ns.get("_incomparable_reason"),
        selected_base=ns.get("_selected_base_commit"),
        printed=printed,
        sidecar=sidecar,
        # Whether the job decided it had coverage data worth publishing. The file
        # is the artifact the post-hook reads to post the GitHub comment and to
        # insert the CI DB row, so its absence is the observable that the
        # abstention really withheld both.
        comment_written=os.path.exists(os.path.join(tmpdir, "coverage_comment.json")),
        report_links=ns.get("report_links"),
    )


def test_absent_info_skips_the_diff_script_and_keeps_the_job_green():
    # The differential script's own precondition exits 1 when llvm_coverage.info
    # is absent, which is exactly the state a failed aggregate merge leaves. If
    # the script is invoked anyway the sub-result is FAIL and its output directory
    # is missing, so the SKIPPED override that reads _measurement_comparable is
    # never reached and the job reddens on a run it promised to report as SKIPPED.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(False, d)
    assert got.invocations == [], f"differential script was invoked: {got.invocations}"
    assert got.diff_res.status == Result.Status.SKIPPED, got.diff_res.status
    assert got.comparable is False
    # The script never ran, so there is no marker to read and nothing to select.
    assert got.selected_base == "", repr(got.selected_base)
    # The assertion that actually pins the invariant: the composite job result.
    parent = Result.create_from(name="LLVM Coverage", results=[got.diff_res])
    assert parent.is_ok(), f"job is not green: {parent.status}"


def test_the_baseline_reads_are_below_the_script_that_writes_their_files():
    # Structural guard, secondary to the behavioural cell below: the differential
    # script is the sole writer of base_llvm_coverage.meta.json and of
    # selected_base_commit.txt, so a read placed above its invocation can only ever
    # observe nothing.
    src = open(_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    block = [
        s
        for s in main_if.body
        if isinstance(s, ast.If) and ast.unparse(s.test) == "not is_master_branch"
    ][0]
    script_lines, read_lines = [], []
    for node in ast.walk(block):
        if not isinstance(node, ast.Call):
            continue
        rendered = ast.unparse(node)
        if "from_commands_run" in rendered and "generate_diff_coverage_report" in rendered:
            script_lines.append(node.lineno)
        if "read_sidecar" in rendered or "selected_base_commit.txt" in rendered:
            read_lines.append(node.lineno)
    assert len(script_lines) == 1, script_lines
    assert read_lines, "neither baseline read was found"
    assert min(read_lines) > script_lines[0], (
        f"a baseline read at line {min(read_lines)} sits above its only writer"
        f" at line {script_lines[0]}"
    )


def test_a_known_incomplete_pr_side_never_runs_the_script_or_prints_a_delta():
    # The PR side being short a shard is knowable from the sidecar before the
    # script runs. Running it anyway spends genhtml on a measurement already known
    # incomparable and prints "Baseline coverage / Current coverage / Delta"
    # between two skip notices, which is how a reader mistakes an abstention for a
    # verdict - the exact confusion this gate exists to remove.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(
            True, d, present_names=[completeness.profile_basename(_GATE_ARTIFACTS[0])]
        )
    assert got.sidecar["complete"] is False, got.sidecar
    assert got.invocations == [], f"differential script was invoked: {got.invocations}"
    assert got.diff_res.status == Result.Status.SKIPPED, got.diff_res.status
    assert got.comparable is False
    assert "COV_B.profdata" in got.reason, got.reason
    assert not any(
        "Baseline coverage" in line or "Delta" in line for line in got.printed
    ), got.printed
    parent = Result.create_from(name="LLVM Coverage", results=[got.diff_res])
    assert parent.is_ok(), f"job is not green: {parent.status}"


def test_a_failed_merge_also_short_circuits_the_script():
    # Same branch, same class: merge_ok=False is a current-side cause too.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, merge_ok=False)
    assert got.invocations == [], f"differential script was invoked: {got.invocations}"
    assert got.diff_res.status == Result.Status.SKIPPED, got.diff_res.status
    assert "aggregate coverage merge failed" in got.reason, got.reason


def test_a_baseline_side_cause_still_runs_the_script_and_uses_the_override():
    # The counterpart that keeps the short-circuit from being over-fixed into
    # "skip every incomparable case", which would make the later override dead
    # code. A baseline-side cause is NOT knowable before the script runs, because
    # that script is what fetches the baseline: so the script MUST run here, and
    # the verdict must come out incomparable afterwards.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, baseline_complete=False)
    assert len(got.invocations) == 1, f"expected one invocation, got {got.invocations}"
    assert "generate_diff_coverage_report.sh" in " ".join(got.invocations[0])
    assert got.comparable is False
    assert "baseline measurement is incomplete" in got.reason, got.reason
    # The override still fires: it is the ONLY thing that turns the OK sub-result
    # from_commands_run produced into SKIPPED on this path.
    assert got.diff_res.status == Result.Status.SKIPPED, got.diff_res.status
    # ... and no delta is printed. The script legitimately ran, so this block IS
    # reached with the measurement already known incomparable; parsing the two
    # summaries here would print three numbers between two abstention notices,
    # which is the reading confusion this gate exists to remove.
    assert not any(
        "Baseline coverage" in line or "Delta" in line for line in got.printed
    ), got.printed


def test_the_short_circuit_reason_cannot_drift_from_the_verdict_function():
    # The job short-circuits on a current-side cause and must report the SAME
    # reason comparable() would. Both read one producer, so this cell pins that
    # they stay one producer.
    incomplete = completeness.build_sidecar(_GATE_ARTIFACTS, ["COV_A.profdata"])
    assert (
        completeness.current_side_reason(incomplete)
        == completeness.comparable(incomplete, None)[1]
    )
    complete = completeness.build_sidecar(
        _GATE_ARTIFACTS, [completeness.profile_basename(n) for n in _GATE_ARTIFACTS]
    )
    # A complete current side yields no current-side reason, while comparable()
    # still rejects on the absent baseline - so the two must NOT be equal here.
    assert completeness.current_side_reason(complete) == ""
    assert completeness.comparable(complete, None)[0] is False


def test_two_complete_sides_reach_a_real_comparable_verdict():
    # The healthy path, asserted on the RUNTIME verdict rather than on the
    # pre-override sub-result status.
    #
    # The property under test is the ORDER of the two baseline reads relative to
    # the differential script that writes their files: the script is the sole
    # writer of base_llvm_coverage.meta.json and selected_base_commit.txt, so a
    # read hoisted above it sees nothing, comparable() takes its absent-baseline
    # branch and the gate abstains on every single run - while emitting exactly
    # the reason string that correct back-compat behaviour also emits, so the CI
    # log of the broken state is indistinguishable from the healthy one. Only a
    # verdict assertion can see that. Do not weaken this cell to a status or
    # invocation-count check.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d)
    assert len(got.invocations) == 1, f"expected one invocation, got {got.invocations}"
    assert "generate_diff_coverage_report.sh" in " ".join(got.invocations[0])
    assert got.comparable is True, f"gate abstained: {got.reason}"
    assert got.reason == "", got.reason
    assert got.selected_base == "a" * 40, repr(got.selected_base)
    assert any("Comparing against baseline commit" in line for line in got.printed), got.printed
    # Reverse direction of the delta suppression above: a comparable run MUST
    # still print all three numbers, so that suppression cannot be widened into
    # "never print a delta". The two stubbed sides differ (84.10 / 84.40), so a
    # swapped-argument regression is visible in the printed values too.
    assert any("Baseline coverage : 84.10%" in l for l in got.printed), got.printed
    assert any("Current coverage  : 84.40%" in l for l in got.printed), got.printed
    assert any("Delta             : +0.30%" in l for l in got.printed), got.printed
    # Reverse direction of the abstention below: a comparable diff-ran verdict must
    # keep the OK sub-result, so the abstention cannot be widened into "always
    # SKIPPED".
    assert got.diff_res.status == Result.Status.OK, got.diff_res.status


def test_baseline_incomplete_with_no_coverable_changes_still_abstains():
    # The one cell in the matrix nobody had driven, and the reason the abstention
    # cannot live inside `if _diff_ran:`.
    #
    # The differential script DOES run here (a baseline-side cause is unknowable
    # before it fetches the baseline), it selects an INCOMPLETE baseline, and it
    # then takes its own `${#patterns[@]} -eq 0` exit because nothing coverable
    # changed - so it never runs genhtml and the report directory is absent. With
    # the abstention nested under `if _diff_ran:`, that made _diff_ran False, the
    # override was skipped, and the sub-result kept the OK from_commands_run gave
    # it: a green verdict for a run that did not judge, which is the exact outcome
    # this gate exists to prevent.
    #
    # _diff_ran is False for three materially different reasons and only one of
    # them - nothing coverable changed on a COMPARABLE run - licenses an OK, so
    # the abstention is guarded on comparability alone and placed after the whole
    # _diff_ran construct, where the degraded arm cannot clobber it either.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, baseline_complete=False, diff_inputs=False)
    assert got.comparable is False
    assert "baseline measurement is incomplete" in got.reason, got.reason
    # The script ran: that is what makes this a BASELINE-side cause rather than
    # the current-side short-circuit, and it is why _diff_ran cannot stand in for
    # "this run produced a verdict".
    assert len(got.invocations) == 1, f"expected one invocation, got {got.invocations}"
    assert got.diff_res.status == Result.Status.SKIPPED, got.diff_res.status
    assert got.reason in got.diff_res.info, (got.reason, got.diff_res.info)
    # A SKIPPED child must keep the JOB green: the abstention states that the gate
    # did not judge, it does not fail the PR author for a tool problem.
    parent = Result.create_from(name="LLVM Coverage", results=got.results)
    assert parent.is_ok(), f"job is not green: {parent.status}"


def test_a_failing_differential_script_stays_red_even_when_incomparable():
    # "The tool broke" and "we cannot judge" genuinely CO-OCCUR, and this is the
    # cell where the abstention must lose. generate_diff_coverage_report.sh writes
    # base_llvm_coverage.meta.json (:64) and selected_base_commit.txt (:100)
    # BEFORE several of its own failure paths - `exit 1` on an empty GitHub
    # compare (:138), and under set -euo pipefail any failure of the two gh api
    # calls (:125-131), of either lcov --extract (:156, :161) or of genhtml
    # (:203). In every one of those the report directory is absent, so _diff_ran
    # is False; and the baseline side is incomparable for EVERY baseline
    # published before this change, because those commits carry no sidecar. Old
    # baseline plus a late script failure is therefore the ordinary post-merge
    # run, not a corner.
    #
    # The asymmetry that makes this reachable at all: the two sibling abstentions
    # (the current-side short-circuit and Print Uncovered Code) CONSTRUCT a fresh
    # Result via create_from, so they cannot destroy a status. This one MUTATES a
    # result that from_commands_run may already have set to FAIL, and SKIPPED is
    # inside is_ok() - so an unguarded set_status turns a real tooling failure
    # green. A failed REPORT must stay RED: merge_llvm_coverage.sh:50-52 says so
    # verbatim, echoed at llvm_coverage_job.py:216-218.
    #
    # Do not delete this as redundant with the sibling SKIPPED cells: all five of
    # those reach the block with diff_res in SKIPPED or OK, so none of them can
    # observe the overwrite.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(
            True, d, baseline_complete=False, diff_inputs=False, script_fails=True
        )
    # The abstention branch really was entered - without this the cell could pass
    # for the trivial reason that comparability never came into question.
    assert got.comparable is False
    assert "baseline measurement is incomplete" in got.reason, got.reason
    # The property under test: the script's own FAIL survives the abstention.
    assert got.diff_res.status == Result.Status.FAIL, got.diff_res.status
    # The assertion that actually pins the invariant, mirroring how the sibling
    # cells assert the composite IS ok.
    assert not Result.create_from(
        name="LLVM Coverage", results=got.results
    ).is_ok(), "a failed differential script must not leave the job green"
    # ...and the ABSTENTION's own print must not have run, so a future refactor
    # cannot satisfy the status assertion while still telling the PR author in the
    # log that the gate merely abstained.
    #
    # Counted rather than asserted absent, because the identical sentence has a
    # SECOND and older producer: llvm_coverage_job.py:355-356 prints it once for
    # every incomparable run, which is what production's own comment at :434-436
    # means by "the reason was already printed above". So one occurrence is the
    # pre-existing report of the cause and is expected here; two would mean the
    # abstention block itself also ran. Measured across three arms: this one
    # prints it ONCE, while the two arms whose script SUCCEEDS print it TWICE, so
    # the count is discriminating.
    _skips = [line for line in got.printed if "Coverage comparison skipped" in line]
    assert len(_skips) == 1, _skips


_NO_CPP = "No C/C++ source files changed"


def test_incomparable_pr_side_does_not_report_no_cpp_changes():
    # The short-circuit is the SOLE reason the two diff inputs are absent on this
    # path, because the differential script that writes them never ran. Testing
    # _diff_inputs_exist before comparability therefore reported "No C/C++ source
    # files changed" twice on a run whose C++ files may well have changed - the
    # job log contradicting itself on the very path this gate exists to serve.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(
            True, d, present_names=[completeness.profile_basename(_GATE_ARTIFACTS[0])]
        )
    assert got.comparable is False
    assert got.print_res.status == Result.Status.SKIPPED, got.print_res.status
    assert got.reason in got.print_res.info, (got.reason, got.print_res.info)
    assert not any(_NO_CPP in line for line in got.printed), got.printed
    # The abstention must not redden anything: SKIPPED counts as OK.
    parent = Result.create_from(name="LLVM Coverage", results=got.results)
    assert parent.is_ok(), f"job is not green: {parent.status}"


def test_an_empty_measurement_does_not_report_no_cpp_changes():
    # Same ordering defect reached by the other current-side cause: the aggregate
    # merge published no llvm_coverage.info at all, so there is nothing to compare
    # - which is emphatically not the same statement as "nothing changed".
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(False, d)
    assert got.comparable is False
    assert got.print_res.status == Result.Status.SKIPPED, got.print_res.status
    assert got.reason in got.print_res.info, (got.reason, got.print_res.info)
    assert not any(_NO_CPP in line for line in got.printed), got.printed
    parent = Result.create_from(name="LLVM Coverage", results=got.results)
    assert parent.is_ok(), f"job is not green: {parent.status}"


def test_a_comparable_run_with_no_coverable_changes_still_reports_no_cpp_changes():
    # The reverse direction, and the cell that keeps the fix from being widened
    # into "always report SKIPPED": both sides are complete and comparable, the
    # script ran and selected a baseline, but nothing coverable changed, so it
    # took its own `${#patterns[@]} -eq 0` exit and wrote neither diff input.
    # That is the one state in which this sentence is TRUE, and it must survive.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, diff_inputs=False)
    assert got.comparable is True, f"gate abstained: {got.reason}"
    assert got.print_res.status == Result.Status.OK, got.print_res.status
    assert _NO_CPP in got.print_res.info, got.print_res.info
    assert any(_NO_CPP in line for line in got.printed), got.printed
    # The diff sub-result too: this cell and the baseline-incomplete one above
    # differ ONLY in comparability, so together they pin that the abstention is
    # keyed on comparability rather than on the absent report directory the two
    # states share.
    assert got.diff_res.status == Result.Status.OK, got.diff_res.status
    parent = Result.create_from(name="LLVM Coverage", results=got.results)
    assert parent.is_ok(), f"job is not green: {parent.status}"


_NO_COV_DATA = "No coverage-relevant changes detected"
_SKIP_COMMENT = "Skipping coverage comment and CI DB row"


def test_current_side_abstention_does_not_report_no_cpp_changes_in_the_comment_branch():
    # The THIRD site of the same ordering defect, in the branch that decides
    # whether to publish the GitHub comment and the CI DB row.
    #
    # The gate on _has_coverage_data is correct - neither is published - but the
    # MESSAGE was selected on _diff_ran alone. On a current-side cause the
    # differential script is deliberately never invoked, so _diff_ran is False for
    # a reason that has nothing to do with C/C++ files, and the run made a claim
    # about the PR's contents that was never established, immediately after having
    # printed the real incompleteness reason.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(
            True, d, present_names=[completeness.profile_basename(_GATE_ARTIFACTS[0])]
        )
    assert got.comparable is False
    assert any(_SKIP_COMMENT in line for line in got.printed), got.printed
    assert got.reason in " ".join(got.printed), (got.reason, got.printed)
    assert not any(_NO_COV_DATA in line for line in got.printed), got.printed
    # And the artifact the post-hook reads really was withheld, so no comment is
    # posted and no CI DB row is inserted from an unjudged run.
    assert got.comment_written is False


def test_baseline_side_abstention_does_not_report_no_cpp_changes_in_the_comment_branch():
    # The baseline-side route to the same branch. Here the script DID run, so the
    # pre-fix _diff_ran test happened to pick the right message when it also
    # produced a report - but not when it took its own no-coverable-files exit,
    # which is the state driven here.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, baseline_complete=False, diff_inputs=False)
    assert got.comparable is False
    assert "baseline measurement is incomplete" in got.reason, got.reason
    assert any(_SKIP_COMMENT in line for line in got.printed), got.printed
    assert not any(_NO_COV_DATA in line for line in got.printed), got.printed
    assert got.comment_written is False


def test_a_comparable_run_with_nothing_coverable_does_report_no_coverage_data():
    # The reverse direction, and the cell that keeps the one-token fix from being
    # widened into "always print the reason": both sides are complete, the script
    # ran, and nothing coverable changed. That is the ONE state in which this
    # sentence is true, so it must survive.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d, diff_inputs=False)
    assert got.comparable is True, f"gate abstained: {got.reason}"
    assert any(_NO_COV_DATA in line for line in got.printed), got.printed
    assert not any(_SKIP_COMMENT in line for line in got.printed), got.printed
    # Still nothing to publish: there is no delta to report.
    assert got.comment_written is False


def test_a_healthy_comparable_run_publishes_the_comment_and_neither_skip_message():
    # The path the whole branch exists to serve. Without this cell the two skip
    # messages could both be suppressed and every assertion above would still
    # pass while the job never published a comment at all.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_diff_gate(True, d)
    assert got.comparable is True, f"gate abstained: {got.reason}"
    assert got.comment_written is True
    assert not any(_NO_COV_DATA in line for line in got.printed), got.printed
    assert not any(_SKIP_COMMENT in line for line in got.printed), got.printed


# --------------------------------------------------------------------------
# The job's report_links block: a published URL must address an artifact that
# exists. The merge phase now legitimately produces no report at all, which is a
# state that did not exist on the merge base (a failed merge exited 1 there and
# reddened the job), so the previously-unconditional append can now point the
# intended green SKIPPED result at a 404.
# --------------------------------------------------------------------------

_FULL_REPORT_URL_TAIL = "generate_llvm_coverage_report/index.html"


def _report_links_nodes():
    """The job's `report_links = []` and the `not is_local_run` block that fills it."""
    src = open(_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    return [
        st
        for st in main_if.body
        if (ast.get_source_segment(src, st) or "").startswith("report_links = []")
        or (
            isinstance(st, ast.If)
            and ast.unparse(st.test) == "not is_local_run"
            and "report_links.append" in ast.unparse(st)
        )
    ]


def _drive_report_links(tmpdir, report_exists, pr_number=4242, branch="some-branch"):
    """Drive the job's own report_links block out of its source.

    Extracted by AST rather than by line range, like the other harnesses here:
    the two statements sit at different nesting depths from anything a single
    dedent could express, and node extraction cannot silently degenerate into an
    IndentationError when either is re-nested.
    """
    keep = _report_links_nodes()
    src = open(_JOB, encoding="utf-8").read()
    assert len(keep) == 2, f"expected the two report_links statements, got {len(keep)}"
    _sliced = "\n".join((ast.get_source_segment(src, s) or "") for s in keep)
    assert (
        _FULL_REPORT_URL_TAIL in _sliced
    ), "harness slice no longer contains the full-report link it asserts on"
    mod = ast.Module(body=keep, type_ignores=[])
    ast.fix_missing_locations(mod)

    if report_exists:
        os.makedirs(os.path.join(tmpdir, "llvm_coverage_html_report"), exist_ok=True)
        with open(
            os.path.join(tmpdir, "llvm_coverage_html_report", "index.html"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write("<html></html>")

    ns = {
        "Path": pathlib.Path,
        "TEMP_DIR": tmpdir,
        "is_local_run": False,
        "pr_number": pr_number,
        "current_commit_sha": "c" * 40,
        "branch": branch,
        "S3_REPORT_BUCKET_HTTP_ENDPOINT": "s3.example.invalid",
        # The diff link's own guard is not under test here; keep it False so the
        # assertions below see the full-report link alone.
        "_diff_ran": False,
        "_measurement_comparable": False,
    }
    exec(compile(mod, _JOB, "exec"), ns)  # noqa: S102 - trusted first-party source
    return ns["report_links"]


def test_no_full_report_link_when_the_merge_produced_no_report():
    # merge_llvm_coverage.sh exits 0 without generating any HTML when
    # merged.profdata is absent, so the intended green SKIPPED result would
    # otherwise advertise a URL that 404s.
    with tempfile.TemporaryDirectory() as d:
        assert _drive_report_links(d, report_exists=False) == []


def test_full_report_link_is_published_when_the_report_exists():
    # The reverse direction, so the guard cannot be widened into "never publish".
    # The URL's shape is asserted verbatim: it is deterministic from the upload
    # path structure, so a change here means a dead link on every healthy run.
    with tempfile.TemporaryDirectory() as d:
        links = _drive_report_links(d, report_exists=True)
    assert links == [
        f"https://s3.example.invalid/PRs/4242/{'c' * 40}/llvm_coverage/{_FULL_REPORT_URL_TAIL}"
    ], links


def test_the_master_branch_link_is_gated_on_the_report_too():
    # The report_links block sits OUTSIDE `if not is_master_branch:`, so a master
    # run with an incomplete measurement publishes the same dead URL. This path
    # uses the REFs/<branch>/<sha> prefix and was untested.
    with tempfile.TemporaryDirectory() as d:
        assert _drive_report_links(d, report_exists=False, pr_number=0) == []
    with tempfile.TemporaryDirectory() as d:
        links = _drive_report_links(d, report_exists=True, pr_number=0, branch="master")
    assert links == [
        f"https://s3.example.invalid/REFs/master/{'c' * 40}/llvm_coverage/{_FULL_REPORT_URL_TAIL}"
    ], links


def test_the_diff_link_still_rides_behind_its_own_guard():
    # The diff link's guard is unchanged; this pins that a comparable diff-ran run
    # still gets BOTH URLs, in the same order.
    mod = ast.Module(body=_report_links_nodes(), type_ignores=[])
    ast.fix_missing_locations(mod)
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, "llvm_coverage_html_report"), exist_ok=True)
        with open(
            os.path.join(d, "llvm_coverage_html_report", "index.html"),
            "w",
            encoding="utf-8",
        ) as f:
            f.write("<html></html>")
        ns = {
            "Path": pathlib.Path,
            "TEMP_DIR": d,
            "is_local_run": False,
            "pr_number": 4242,
            "current_commit_sha": "c" * 40,
            "branch": "some-branch",
            "S3_REPORT_BUCKET_HTTP_ENDPOINT": "s3.example.invalid",
            "_diff_ran": True,
            "_measurement_comparable": True,
        }
        exec(compile(mod, _JOB, "exec"), ns)  # noqa: S102 - trusted first-party source
    assert [link.rsplit("/llvm_coverage/", 1)[1] for link in ns["report_links"]] == [
        _FULL_REPORT_URL_TAIL,
        "generate_llvm_coverage_diff_report/index_diff.html",
    ], ns["report_links"]


# --------------------------------------------------------------------------
# Row 11: the producer-side filename and the consumer-side expected name agree,
# driven through a real dump()/get() round-trip.
# --------------------------------------------------------------------------


def _round_trip_job_config(provides):
    """Return Info().job_config as a job body really sees it."""
    from ci.praktika._environment import _Environment

    os.makedirs("ci/tmp", exist_ok=True)
    jc = Job.Config(name="ut", runs_on=["x"], command="c", provides=provides)
    _Environment(
        WORKFLOW_NAME="w",
        JOB_NAME="ut",
        REPOSITORY="a/b",
        BRANCH="master",
        SHA="deadbeef",
        PR_NUMBER=0,
        EVENT_TYPE="push",
        JOB_OUTPUT_STREAM="",
        EVENT_FILE_PATH="",
        CHANGE_URL="",
        COMMIT_URL="",
        BASE_BRANCH="",
        RUN_URL="",
        RUN_ID="",
        INSTANCE_ID="",
        INSTANCE_TYPE="",
        INSTANCE_LIFE_CYCLE="",
        LOCAL_RUN=False,
        PR_BODY="",
        PR_TITLE="",
        USER_LOGIN="",
        FORK_NAME="",
        PR_LABELS=[],
        EVENT_TIME="",
        JOB_CONFIG=jc,
    ).dump()
    return _Environment.get().JOB_CONFIG


def test_job_config_survives_serialization_only_as_a_dict():
    # An in-memory Job.Config passes under BOTH the attribute and the subscript
    # spelling, so an in-memory cell here would be vacuous. This drives the real
    # round-trip, which is where the attribute spelling breaks.
    cwd = os.getcwd()
    try:
        os.chdir(_REPO_ROOT)
        got = _round_trip_job_config(["LLVM_COVERAGE_FILE_ut"])
    finally:
        os.chdir(cwd)
    assert isinstance(got, dict)
    assert got["provides"][0] == "LLVM_COVERAGE_FILE_ut"
    try:
        got.provides
        raise AssertionError("attribute access unexpectedly worked")
    except AttributeError:
        pass


def test_producer_and_consumer_agree_on_the_profile_name():
    name = "LLVM_COVERAGE_FILE_ut"
    cwd = os.getcwd()
    try:
        os.chdir(_REPO_ROOT)
        got = _round_trip_job_config([name])
    finally:
        os.chdir(cwd)
    producer_side = f"./{got['provides'][0]}.profdata"
    consumer_side = completeness.profile_basename(name)
    assert os.path.basename(producer_side) == consumer_side


def test_every_producer_reads_provides_as_a_dict_key():
    for path in (_UT_JOB, _FT_JOB, _IT_JOB):
        src = open(path, encoding="utf-8").read()
        assert '["provides"]' in src, path
        assert ".job_config.provides" not in src, path


def _drive_ut_naming(provides, profraw_files, tmpdir, job_path=None):
    """Execute the real coverage bookkeeping of unit_tests_job.py.

    Returns the derived `merged_file`, or None when the job never derives one.
    The statements are taken from the job source and run in their real order, so
    the nesting under the `.profraw` guard is exercised rather than described.
    from_gtest_run/complete_job are dropped because they need a built gtest
    binary; everything the naming path touches is real.
    """
    src = open(job_path or _UT_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    keep = []
    for st in main_if.body:
        txt = ast.get_source_segment(src, st) or ""
        if txt.startswith(
            (
                "R = Result.from_gtest_run",
                "R.complete_job",
                "parser",
                "args =",
                "os.environ",
                "job_name =",
                'if "asan"',
                "profraw_files",
            )
        ):
            continue
        keep.append(st)
    mod = ast.Module(body=keep, type_ignores=[])
    ast.fix_missing_locations(mod)
    code = compile(mod, job_path or _UT_JOB, "exec")

    class _Info:
        job_config = {"provides": provides}

    ns = {
        "os": os,
        "Info": lambda: _Info(),
        "Shell": types.SimpleNamespace(
            get_output=lambda *a, **k: "", check=lambda *a, **k: False
        ),
        "profraw_files": list(profraw_files),
    }
    cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        exec(code, ns)
    finally:
        os.chdir(cwd)
    return ns.get("merged_file")


def test_unit_test_job_names_no_profile_when_there_is_no_coverage_to_name():
    # The six sanitizer Unit tests ParamSets declare no artifact and emit no
    # .profraw. The naming asserts sit between from_gtest_run() and
    # complete_job(), so raising here does not merely mis-report - it loses the
    # gtest result entirely.
    with tempfile.TemporaryDirectory() as d:
        got = _drive_ut_naming([], [], d)
    assert got is None


def test_unit_test_job_still_names_the_profile_after_its_own_artifact():
    # Reverse direction: the one instrumented ParamSet must keep deriving exactly
    # the name the consumer side expects, so moving the block cannot silently stop
    # naming the profile at all.
    name = "LLVM_COVERAGE_FILE"
    with tempfile.TemporaryDirectory() as d:
        raw = os.path.join(d, "one.profraw")
        with open(raw, "w") as f:
            f.write("data")
        got = _drive_ut_naming([name], [raw], d)
    assert got == f"./{name}.profdata"
    assert os.path.basename(got) == completeness.profile_basename(name)


def test_unit_test_job_naming_is_nested_under_the_profraw_guard():
    # Structural companion to the two cells above: catches a re-hoist to the
    # top level of __main__, where the asserts run unconditionally.
    src = open(_UT_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    main_if = [n for n in tree.body if isinstance(n, ast.If)][-1]
    assert not [
        s for s in main_if.body if isinstance(s, ast.Assert)
    ], "coverage asserts must not be direct children of __main__"
    guards = [
        s
        for s in main_if.body
        if isinstance(s, ast.If) and ast.unparse(s.test) == "profraw_files"
    ]
    assert guards, "expected an `if profraw_files:` guard"
    nested = [n for g in guards for n in ast.walk(g) if isinstance(n, ast.Assert)]
    assert (
        len(nested) == 2
    ), f"expected both asserts inside the guard, got {len(nested)}"


def test_the_six_non_coverage_unit_test_jobs_declare_no_artifact():
    # The premise behind the cells above, read off the real job configs rather
    # than assumed: only the instrumented ParamSet provides an artifact.
    sys.path.insert(0, _REPO_ROOT)
    from ci.defs.job_configs import JobConfigs

    plain = list(JobConfigs.unittest_jobs)
    assert len(plain) == 6, [j.name for j in plain]
    assert all(j.provides == [] for j in plain), [(j.name, j.provides) for j in plain]
    coverage = list(JobConfigs.unittest_llvm_coverage_job)
    assert len(coverage) == 1, [j.name for j in coverage]
    assert coverage[0].provides == ["LLVM_COVERAGE_FILE"], coverage[0].provides


def test_local_runs_get_a_job_config_too():
    # Without this a locally-run coverage shard dies with TypeError before
    # merging, on praktika's own supported local path.
    src = open(
        os.path.join(_CI_ROOT, "praktika", "runner.py"), encoding="utf-8"
    ).read()
    local = src[src.index("def generate_local_run_environment") :]
    local = local[: local.index("\n    @")] if "\n    @" in local else local
    assert "JOB_CONFIG=job" in local


def test_every_llvm_producer_paramset_provides_exactly_one_artifact():
    # `provides[0]` is only well defined because of this.
    assert len(LLVM_ARTIFACTS_LIST) >= 18
    assert len(set(LLVM_ARTIFACTS_LIST)) == len(LLVM_ARTIFACTS_LIST)


# --------------------------------------------------------------------------
# Baseline selection: prefer a complete, manifest-matching ancestor, but keep
# today's first-match walk as the back-compat fallback.
# --------------------------------------------------------------------------


def _run_selector(ancestors):
    """Drive the real selector loop against a synthetic S3.

    `ancestors` is an ordered list of (sha, has_info, sidecar_or_None); the wget
    shim serves them. Returns the sha the script selected, or None.
    """
    with tempfile.TemporaryDirectory() as d:
        bin_dir = os.path.join(d, "bin")
        os.makedirs(bin_dir)
        s3 = os.path.join(d, "s3")
        os.makedirs(s3)
        our = _sidecar()
        for sha, has_info, sidecar in ancestors:
            if has_info:
                with open(os.path.join(s3, f"{sha}.info"), "w") as f:
                    f.write("SF:/a.cpp\nDA:1,1\nend_of_record\n")
            if sidecar is not None:
                with open(os.path.join(s3, f"{sha}.meta.json"), "w") as f:
                    json.dump(sidecar, f)

        # A wget shim that maps the selector's URLs onto that directory.
        with open(os.path.join(bin_dir, "wget"), "w") as f:
            f.write(
                "#!/usr/bin/env python3\n"
                "import os, shutil, sys\n"
                f"S3 = {s3!r}\n"
                "args = sys.argv[1:]\n"
                "spider = '--spider' in args\n"
                "out = None\n"
                "if '-O' in args: out = args[args.index('-O') + 1]\n"
                "url = [a for a in args if a.startswith('http')][0]\n"
                "sha = url.split('/REFs/master/')[1].split('/')[0]\n"
                "kind = '.info' if url.endswith('llvm_coverage.info') else '.meta.json'\n"
                "src = os.path.join(S3, sha + kind)\n"
                "if not os.path.exists(src):\n"
                "    sys.stderr.write('404 Not Found\\n'); sys.exit(8)\n"
                "if spider:\n"
                "    sys.stderr.write('200 OK\\n'); sys.exit(0)\n"
                "shutil.copy2(src, out)\n"
            )
        os.chmod(os.path.join(bin_dir, "wget"), 0o755)

        work = os.path.join(d, "work")
        os.makedirs(os.path.join(work, "ci", "tmp"))
        tmp = os.path.join(work, "ci", "tmp")
        with open(os.path.join(tmp, "llvm_coverage.info"), "w") as f:
            f.write("SF:/a.cpp\nDA:1,1\nend_of_record\n")
        completeness.write_sidecar(
            os.path.join(tmp, completeness.SIDECAR_BASENAME), our
        )
        # Stop after selection: the rest of the script needs a git tree.
        lines = open(_SELECT_SH, encoding="utf-8").read().splitlines(True)
        end = next(
            i for i, l in enumerate(lines) if "selected_base_commit.txt" in l
        )
        with open(os.path.join(work, "select.sh"), "w") as f:
            f.writelines(lines[: end + 1])

        env = dict(
            os.environ,
            PATH=bin_dir + os.pathsep + os.environ["PATH"],
            PREV_30_COMMITS=",".join(a[0] for a in ancestors),
            CURRENT_COMMIT="cur",
            BASE_COMMIT=ancestors[0][0],
            BRANCH="pr",
            BASE_BRANCH="master",
            WORKSPACE_PATH=work,
        )
        r = subprocess.run(
            ["bash", "select.sh"], cwd=work, env=env, capture_output=True, text=True
        )
        sel = os.path.join(tmp, "selected_base_commit.txt")
        return (
            open(sel).read().strip() if os.path.exists(sel) else None,
            r.returncode,
            r.stdout,
        )


def test_selector_prefers_a_complete_manifest_matching_ancestor_over_a_nearer_one():
    # Pass 1. Most master runs are incomplete, so taking the first ancestor with
    # an .info would make the gate abstain on the majority of runs.
    complete = _sidecar()
    incomplete = _sidecar(present=_ALL_PRESENT[:-1])
    sel, rc, _ = _run_selector(
        [("near", True, incomplete), ("far", True, complete)]
    )
    assert rc == 0
    assert sel == "far"


def test_selector_falls_back_to_the_first_info_when_no_ancestor_is_complete():
    # Pass 2, byte-for-byte the old behaviour. This is what keeps the change
    # backward compatible: no master commit published before it has a sidecar, so
    # until master republishes pass 1 finds nothing and the job reports SKIPPED
    # rather than failing every PR.
    sel, rc, _ = _run_selector([("near", True, None), ("far", True, None)])
    assert rc == 0
    assert sel == "near"


def test_selector_pass_one_rejects_a_mismatched_manifest():
    old_names = _NAMES[:18]
    other_manifest = _sidecar(
        names=old_names,
        present=[completeness.profile_basename(n) for n in old_names],
    )
    assert other_manifest["complete"] is True
    complete = _sidecar()
    # The nearer ancestor is complete but measured a DIFFERENT manifest.
    sel, rc, _ = _run_selector(
        [("near", True, other_manifest), ("far", True, complete)]
    )
    assert rc == 0
    assert sel == "far"


def test_selector_records_the_commit_it_actually_chose():
    # The job cannot re-derive it: its own base_commit_sha is the NEAREST
    # ancestor, generally not the selected one.
    sel, rc, _ = _run_selector([("a", False, None), ("b", True, None)])
    assert rc == 0
    assert sel == "b"


def test_job_reads_the_selected_commit_rather_than_rewalking_s3():
    src = open(_JOB, encoding="utf-8").read()
    assert "selected_base_commit.txt" in src
    assert "base_llvm_coverage.meta.json" in src


# --------------------------------------------------------------------------
# Registration: both job-filter layers, and the one that fires first
# --------------------------------------------------------------------------


def test_the_shared_module_is_registered_in_both_filter_layers():
    layer1 = open(_FILTER_JOB, encoding="utf-8").read()
    layer2 = open(_JOB_CONFIGS, encoding="utf-8").read()
    # Layer 1 decides whether the coverage FAMILY runs at all, and it is
    # consulted BEFORE any digest, so registering only the digest is not enough.
    assert "ci/jobs/scripts/llvm_coverage_completeness.py" in layer1
    assert "./ci/jobs/scripts/llvm_coverage_completeness.py" in layer2


def test_the_ft_completion_parser_is_registered_in_layer_one():
    layer1 = open(_FILTER_JOB, encoding="utf-8").read()
    assert "ci/jobs/scripts/functional_tests_results.py" in layer1


def test_the_runner_local_parity_fix_is_deliberately_not_registered():
    # CI always takes the _setup_env branch (local_run = not args.ci), so CI never
    # executes the changed code; digesting it would schedule every coverage shard
    # on each runner.py edit for no signal.
    layer1 = open(_FILTER_JOB, encoding="utf-8").read()
    layer2 = open(_JOB_CONFIGS, encoding="utf-8").read()
    assert "ci/praktika/runner.py" not in layer1
    assert "./ci/praktika/runner.py" not in layer2
    main = open(os.path.join(_CI_ROOT, "praktika", "__main__.py"), encoding="utf-8").read()
    assert "local_run=not args.ci" in main


# --------------------------------------------------------------------------
# Sidecar shape
# --------------------------------------------------------------------------


def test_sidecar_round_trips_through_disk():
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, completeness.SIDECAR_BASENAME)
        original = _sidecar()
        completeness.write_sidecar(p, original)
        assert completeness.read_sidecar(p) == original
        # It must be plain JSON so the selector can read it with a one-liner.
        json.loads(open(p).read())


def test_sidecar_records_which_shards_were_missing():
    s = _sidecar(present=_ALL_PRESENT[:-2])
    assert s["missing"] == sorted(_ALL_PRESENT[-2:])
    assert s["complete"] is False


def test_a_complete_sidecar_says_so():
    s = _sidecar()
    assert s["complete"] is True
    assert s["missing"] == []
    assert s["unexpected"] == []
    assert s["schema_version"] == completeness.SCHEMA_VERSION


def test_profile_basename_rejects_a_missing_artifact_name():
    # A silent None here would name the profile "None.profdata" and turn a naming
    # bug into a phantom incompleteness verdict on every run.
    for bad in (None, "", 0, []):
        try:
            completeness.profile_basename(bad)
            raise AssertionError(f"accepted {bad!r}")
        except AssertionError as e:
            if "accepted" in str(e):
                raise


def test_skipped_status_counts_as_ok_so_the_job_stays_green():
    # The whole SKIPPED-not-FAIL design rests on this.
    r = Result(name="x", status=Result.Status.SKIPPED)
    assert r.is_ok() is True
