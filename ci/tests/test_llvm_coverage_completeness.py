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

import json
import os
import shutil
import subprocess
import sys
import tempfile

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


def test_runner_emits_the_marker_only_when_it_completed_and_kept_its_workers():
    src = open(_CH_TEST, encoding="utf-8").read()
    assert "Coverage run completed all selected tests." in src
    # Both halves of the predicate must be present, and the existing marker text
    # and exit contract untouched.
    idx = src.index("Coverage run completed all selected tests.")
    window = src[idx - 400 : idx]
    assert "total_tests_run != 0" in window
    assert "runner_process_killed.is_set()" in window
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
    src = open(_JOB, encoding="utf-8").read()
    assert "if _diff_inputs_exist and not _measurement_comparable:" in src
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


def test_job_treats_an_absent_info_as_incomplete_rather_than_comparing_nothing():
    src = open(_JOB, encoding="utf-8").read()
    assert 'if not Path(f"{TEMP_DIR}/llvm_coverage.info").exists():' in src
    assert "nothing to compare" in src


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
