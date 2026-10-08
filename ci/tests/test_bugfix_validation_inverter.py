"""
Tests for `ci.jobs.functional_tests.invert_bugfix_validation_status`.

The bugfix-validation inverter flips per-test `FAIL`/`OK` so that a regression
test for a bug, which is expected to `FAIL` on master HEAD, is reported as
"bug reproduced". When the test instead passes on master HEAD, the bug did
not reproduce on this arch (no-repro): the inverter reports `SKIPPED` and
returns True, so the caller propagates `SKIPPED` to the top-level result and
the per-arch job exits 0 without being counted as a validation - another
arch can still validate the bug (the per-arch contract, PR #103541).

When the run itself failed catastrophically (status `ERROR`, e.g. runner
killed mid-flight or server crashed without a synthetic `Server died` leaf
reaching `test_result.results`), the inverter must preserve `ERROR` rather
than overwrite it with a validation verdict.

This module also tests `reconcile_bugfix_crash_repro`, which runs BEFORE the
inverter and folds a build type's fatal-log rows into its per-test result. A
`BLOCKER` fatal on the master-HEAD binary is the bug crashing the server (with
`-fno-sanitize-recover=all` a reproduced UBSan bug aborts the runner, poisoning
the per-test rows with `ERROR`), so it downgrades those `ERROR` rows to `FAIL`
for the inverter to flip into a reproduction; a run that ends in `ERROR` with
no `BLOCKER` fatal (genuine infra failure) is preserved as inconclusive. The
same reconciliation now runs on every build type (`build_types[0]` and each of
`build_types[1:]`); the tests below pin the helper's behaviour on the
crash-repro input that the later build types depend on. They call the helper
directly (not through `main`'s build-type loop, which swaps binaries and reads
real server logs), so they guard the reconciliation logic itself rather than
the caller wiring.

The last group covers `attach_post_verdict_artifacts`, which runs AFTER the
inverter: the COLLECT_LOGS stage appends artifact-collection rows, and
`extend_sub_results` re-derives the parent status from its children, so a failed
log dump used to overwrite the validation verdict and block the PR.

See ClickHouse/ClickHouse#105789, #103541, #110158 and #113397.
"""

import ast
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.functional_tests import (
    attach_post_verdict_artifacts,
    invert_bugfix_validation_status,
    reconcile_bugfix_crash_repro,
)
from ci.praktika.result import Result

JOBS_DIR = Path(__file__).resolve().parent.parent / "jobs"


def _make_leaf(name, status, info=""):
    return Result(name=name, status=status, info=info)


def _make_log_check(name, status):
    """A server-log / runner health-check row, as produced by
    `check_fatal_messages_in_logs` and labelled `LOG_CHECK`."""
    leaf = Result(name=name, status=status)
    leaf.set_label(Result.Label.LOG_CHECK)
    return leaf


def _make_fatal(name="Sanitizer assert or Fatal messages in server logs"):
    """A `BLOCKER` sanitizer/fatal row, as produced by the fatal branches of
    `check_fatal_messages_in_logs` (clickhouse_proc.py): FAIL + BLOCKER."""
    leaf = Result(name=name, status=Result.Status.FAIL)
    leaf.set_label(Result.Label.BLOCKER)
    return leaf


def _labels(leaf):
    return [
        lbl.get("name") if isinstance(lbl, dict) else lbl
        for lbl in leaf.ext.get("labels", [])
    ]


def _make_outer(status, results=None, info=""):
    return Result(
        name="Tests",
        status=status,
        results=results or [],
        info=info,
    )


def test_single_fail_is_flipped_to_ok_and_outer_becomes_success():
    """Regression test FAILed on master -> bug reproduced -> overall OK."""
    leaf = _make_leaf("01234_regression_test", Result.Status.FAIL,
                      info="server died with SIGSEGV")
    outer = _make_outer(Result.Status.FAIL, [leaf])

    invert_bugfix_validation_status(outer)

    assert leaf.status == Result.Status.OK
    assert outer.status == Result.Status.OK


def test_single_ok_is_no_repro_and_outer_becomes_skipped():
    """Regression test PASSed on master -> bug did not reproduce on this arch
    -> overall SKIPPED (not FAIL), and the inverter signals no-repro so the
    caller propagates SKIPPED to the top-level result. This is the per-arch
    contract: another arch can still validate the bug.
    """
    leaf = _make_leaf("01234_regression_test", Result.Status.OK)
    outer = _make_outer(Result.Status.OK, [leaf])

    no_repro = invert_bugfix_validation_status(outer)

    assert no_repro is True
    assert leaf.status == Result.Status.FAIL
    assert outer.status == Result.Status.SKIPPED
    assert "bugfix validation N/A" in outer.info


def test_mixed_fail_and_ok_treats_any_fail_as_bug_reproduced():
    """At least one FAIL on master is enough to declare the bug reproduced."""
    leaf_fail = _make_leaf("01234_regression_test", Result.Status.FAIL)
    leaf_ok = _make_leaf("99999_unrelated_test", Result.Status.OK)
    outer = _make_outer(Result.Status.FAIL, [leaf_fail, leaf_ok])

    invert_bugfix_validation_status(outer)

    assert leaf_fail.status == Result.Status.OK
    assert leaf_ok.status == Result.Status.FAIL
    assert outer.status == Result.Status.OK


def test_server_died_synthetic_fail_leaf_treated_as_bug_reproduced():
    """Mirrors the leshikus PR #105643 flow: parser synthesises a `Server
    died` FAIL leaf for `runner_exit_code in {STOP_TESTING_EXIT_CODE, 137,
    143}`. The inverter must flip it to OK so the bugfix check passes.
    """
    leaf = _make_leaf("Server died", Result.Status.FAIL, info="Server died")
    outer = _make_outer(Result.Status.FAIL, [leaf])

    invert_bugfix_validation_status(outer)

    assert leaf.status == Result.Status.OK
    assert outer.status == Result.Status.OK


def test_error_status_with_empty_results_preserves_error():
    """The regression in #105789: the runner did not finish, no per-test
    results were emitted, status is `ERROR`. The inverter must NOT
    overwrite that with `FAIL` "Failed to reproduce the bug".
    """
    outer = _make_outer(
        Result.Status.ERROR,
        results=[],
        info="The test runner was terminated unexpectedly",
    )

    invert_bugfix_validation_status(outer)

    # The honest ERROR is preserved.
    assert outer.status == Result.Status.ERROR
    # The original info is preserved (no "Failed to reproduce" appended).
    assert outer.info == "The test runner was terminated unexpectedly"
    assert "Failed to reproduce the bug" not in outer.info


def test_error_status_with_partial_results_preserves_error():
    """Same as the empty case, but with partial per-test data from an
    interrupted run. We must not flip OK leaves to FAIL on a run that
    never completed.
    """
    leaf = _make_leaf("01234_some_other_test", Result.Status.OK)
    outer = _make_outer(
        Result.Status.ERROR,
        results=[leaf],
        info="The test runner was terminated unexpectedly",
    )

    invert_bugfix_validation_status(outer)

    # Outer status preserved.
    assert outer.status == Result.Status.ERROR
    # Leaf status not flipped (the run was inconclusive, so flipping
    # a passing test to FAIL would be incorrect).
    assert leaf.status == Result.Status.OK
    # Leaf is still labelled XFAIL so json.html renders it consistently.
    leaf_labels = [
        lbl.get("name") if isinstance(lbl, dict) else lbl
        for lbl in leaf.ext.get("labels", [])
    ]
    assert Result.Label.XFAIL in leaf_labels


def test_xfail_label_is_applied_to_each_leaf_on_inversion():
    """Both flipped and non-flipped leaves should get the XFAIL label so
    json.html renders them consistently in the bugfix-validation report.
    """
    leaf_fail = _make_leaf("01234_a", Result.Status.FAIL)
    leaf_ok = _make_leaf("01234_b", Result.Status.OK)
    leaf_skipped = _make_leaf("01234_c", Result.Status.SKIPPED)
    outer = _make_outer(
        Result.Status.FAIL, [leaf_fail, leaf_ok, leaf_skipped]
    )

    invert_bugfix_validation_status(outer)

    for leaf in (leaf_fail, leaf_ok, leaf_skipped):
        labels = [
            lbl.get("name") if isinstance(lbl, dict) else lbl
            for lbl in leaf.ext.get("labels", [])
        ]
        assert Result.Label.XFAIL in labels, (
            f"XFAIL label missing on {leaf.name} (status={leaf.status})"
        )


def test_empty_results_with_ok_outer_is_no_repro_skipped():
    """If the outer status is OK and there are no per-test results, no bug was
    reproduced on this arch -> SKIPPED + no-repro signal. (Realistic scenario:
    the bug-fix PR runs but the test passes on master HEAD on this arch.)
    """
    outer = _make_outer(Result.Status.OK, results=[], info="")

    no_repro = invert_bugfix_validation_status(outer)

    assert no_repro is True
    assert outer.status == Result.Status.SKIPPED
    assert "bugfix validation N/A" in outer.info


def test_clean_log_checks_are_not_flipped_to_failures():
    """Clean health checks ("Lost s3 keys", "OOM in dmesg", ...) are OK and
    must stay OK: they are not test cases and must not become spurious xfail
    failures when the bug is not reproduced.
    """
    test_ok = _make_leaf("01234_regression_test", Result.Status.OK)
    log_checks = [
        _make_log_check("Exception in test runner", Result.Status.OK),
        _make_log_check("Lost s3 keys", Result.Status.OK),
        _make_log_check("OOM in dmesg", Result.Status.OK),
    ]
    outer = _make_outer(Result.Status.OK, [test_ok, *log_checks])

    invert_bugfix_validation_status(outer)

    # The real test row is flipped (bug not reproduced).
    assert test_ok.status == Result.Status.FAIL
    # The health checks stay OK and are not labelled XFAIL.
    for leaf in log_checks:
        assert leaf.status == Result.Status.OK
        assert Result.Label.XFAIL not in _labels(leaf)
    # Per-arch contract: no test row reproduced the bug here, so the outer
    # status is SKIPPED (not FAIL); another arch can still validate.
    assert outer.status == Result.Status.SKIPPED
    assert "bugfix validation N/A" in outer.info


def test_clean_log_checks_do_not_mask_a_reproduced_bug():
    """A reproduced bug (test FAIL on master) is still reported even when
    health-check rows are present and clean.
    """
    test_fail = _make_leaf("01234_regression_test", Result.Status.FAIL)
    log_check = _make_log_check("Lost s3 keys", Result.Status.OK)
    outer = _make_outer(Result.Status.FAIL, [test_fail, log_check])

    invert_bugfix_validation_status(outer)

    assert test_fail.status == Result.Status.OK
    assert log_check.status == Result.Status.OK
    assert outer.status == Result.Status.OK


def test_log_check_failure_counts_as_reproduced_bug():
    """A fatal / sanitizer assert / lost key on the validated binary is the
    bug reproducing, so it is flipped to OK and the job passes, even when no
    plain test row failed.
    """
    test_ok = _make_leaf("01234_regression_test", Result.Status.OK)
    log_fail = _make_log_check(
        "Sanitizer assert or Fatal messages in server logs", Result.Status.FAIL
    )
    outer = _make_outer(Result.Status.FAIL, [test_ok, log_fail])

    invert_bugfix_validation_status(outer)

    # The fatal is treated as a reproduction: flipped to OK and labelled XFAIL.
    assert log_fail.status == Result.Status.OK
    assert Result.Label.XFAIL in _labels(log_fail)
    assert outer.status == Result.Status.OK
    assert "Failed to reproduce" not in outer.info


# ---------------------------------------------------------------------------
# reconcile_bugfix_crash_repro (PR #110158): runs on every build type, before
# the inverter. Its crash-repro downgrade is what the later-build-type
# (`build_types[1:]`) flow relies on - the PR's own WITH FILL overflow
# reproduces on `build_types[0]` and short-circuits before the later loop, so
# no bugfix-validation run exercises that helper call there. These tests pin
# the helper's behaviour directly on the runner_level_error + per-test ERROR +
# BLOCKER fatal input that flow feeds it.
# ---------------------------------------------------------------------------


def test_reconcile_crash_repro_downgrades_runner_error_to_fail():
    """The later-build-type scenario that PR #110158 repairs: a sanitizer crash
    on the master-HEAD binary aborts the runner, so the build type's result is
    `ERROR` with per-test `ERROR` rows, and a `BLOCKER` fatal is present. The
    reconciler must downgrade the per-test `ERROR`s to `FAIL` and recompute the
    aggregate to `FAIL` (not `ERROR`), so the inverter counts a reproduction.
    """
    poisoned = _make_leaf("01234_regression_test", Result.Status.ERROR)
    bt_result = _make_outer(Result.Status.ERROR, [poisoned])
    fatals = [_make_fatal()]

    crash_repro = reconcile_bugfix_crash_repro(bt_result, fatals)

    assert crash_repro is True
    # Per-test ERROR downgraded to FAIL.
    assert poisoned.status == Result.Status.FAIL
    # Aggregate is FAIL, not ERROR: the runner-level ERROR is NOT restored
    # because a crash reproduction was detected.
    assert bt_result.status == Result.Status.FAIL
    # The fatal row is folded in for the report.
    assert any(r.has_label(Result.Label.BLOCKER) for r in bt_result.results)


def test_reconcile_crash_repro_then_inverter_flips_to_ok():
    """End-to-end for the later build type: after the reconciler turns the
    poisoned `ERROR` run into `FAIL`, the inverter flips it to a successful
    reproduction (`OK`) instead of preserving it as inconclusive `ERROR`.
    """
    poisoned = _make_leaf("01234_regression_test", Result.Status.ERROR)
    bt_result = _make_outer(Result.Status.ERROR, [poisoned])

    reconcile_bugfix_crash_repro(bt_result, [_make_fatal()])
    no_repro = invert_bugfix_validation_status(bt_result)

    assert no_repro is not True
    assert bt_result.status == Result.Status.OK


def test_reconcile_infra_error_without_blocker_is_preserved():
    """#105789 contract: a run that ends in `ERROR` with no `BLOCKER` fatal is
    a genuine infra failure, not a reproduction. The reconciler must NOT
    downgrade its `ERROR` rows and must restore the runner-level `ERROR`.
    """
    err_row = _make_leaf("01234_regression_test", Result.Status.ERROR)
    bt_result = _make_outer(Result.Status.ERROR, [err_row])
    # A clean (OK) health-check row - no BLOCKER fatal.
    clean = [_make_log_check("Lost s3 keys", Result.Status.OK)]

    crash_repro = reconcile_bugfix_crash_repro(bt_result, clean)

    assert crash_repro is False
    # ERROR row untouched.
    assert err_row.status == Result.Status.ERROR
    # Runner-level ERROR restored (not left as OK/FAIL by extend_sub_results).
    assert bt_result.status == Result.Status.ERROR


def test_reconcile_ok_run_with_blocker_fatal_still_flips_to_fail():
    """A clean per-test run (OK) that nevertheless produced a `BLOCKER` fatal in
    the server log is the bug reproducing via a crash: the reconciler folds the
    fatal in and the aggregate becomes FAIL, which the inverter later flips to
    a reproduction. (No runner-level ERROR to restore here.)
    """
    ok_row = _make_leaf("01234_regression_test", Result.Status.OK)
    bt_result = _make_outer(Result.Status.OK, [ok_row])

    crash_repro = reconcile_bugfix_crash_repro(bt_result, [_make_fatal()])

    assert crash_repro is True
    # No per-test ERROR to downgrade; the OK row stays OK.
    assert ok_row.status == Result.Status.OK
    # The BLOCKER fatal makes the aggregate FAIL.
    assert bt_result.status == Result.Status.FAIL


# ---------------------------------------------------------------------------
# attach_post_verdict_artifacts (#113397): the COLLECT_LOGS stage appends
# artifact-collection rows after the inverter has decided the verdict, and
# `extend_sub_results` re-derives the parent status from its children. On
# #113397 the PR's own bug hung `DROP TABLE ... SYNC`, `clickhouse stop` timed
# out, the still-running server held its status-file lock and every
# `clickhouse local` system-table dump failed - so a single "Scraping system
# tables" FAIL row overwrote an `OK` verdict on both arches, and
# `any_bugfix_validation_passed` (strict `is_success`) blocked a PR whose bug
# had in fact been validated twice.
# ---------------------------------------------------------------------------


def _make_artifact_row(name="Scraping system tables"):
    """A post-verdict artifact-collection row, as appended by the COLLECT_LOGS
    stage from `ClickHouseProc.extra_tests_results` (clickhouse_proc.py)."""
    return Result(
        name=name,
        status=Result.Status.FAIL,
        info="Failed to dump system table: query_log",
    )


def test_artifact_row_cannot_unvalidate_a_reproduced_bug():
    """#113397: a reproduction must survive a failed artifact dump.

    `is_success` (not `is_ok`) is asserted because that is what
    `any_bugfix_validation_passed` in `new_tests_check.py` uses to decide
    whether any arch validated the bug.
    """
    leaf = _make_leaf("04700_regression_test", Result.Status.FAIL)
    outer = _make_outer(Result.Status.FAIL, [leaf])

    assert invert_bugfix_validation_status(outer) is False
    assert outer.is_success()

    attach_post_verdict_artifacts(outer, [_make_artifact_row()], preserve_verdict=True)

    assert outer.status == Result.Status.OK
    assert outer.is_success()
    # The diagnostic row is still in the report.
    assert [r.name for r in outer.results][-1] == "Scraping system tables"
    assert outer.results[-1].status == Result.Status.FAIL


def test_artifact_row_does_not_redden_a_no_repro_job():
    """A no-repro arch reports SKIPPED so it exits 0 without counting as a
    validation. A failed artifact dump must not turn it into a red job, nor
    into a validation.
    """
    leaf = _make_leaf("04700_regression_test", Result.Status.OK)
    outer = _make_outer(Result.Status.OK, [leaf])

    assert invert_bugfix_validation_status(outer) is True
    assert outer.status == Result.Status.SKIPPED

    attach_post_verdict_artifacts(outer, [_make_artifact_row()], preserve_verdict=True)

    assert outer.status == Result.Status.SKIPPED
    assert outer.is_ok()
    assert not outer.is_success()


def test_artifact_row_does_not_downgrade_an_inconclusive_error():
    """#105789 contract: an inconclusive run keeps `ERROR`. Downgrading it to
    `FAIL` would report a verdict where the inverter deliberately reported
    none.
    """
    outer = _make_outer(
        Result.Status.ERROR,
        results=[],
        info="The test runner was terminated unexpectedly",
    )

    invert_bugfix_validation_status(outer)
    assert outer.status == Result.Status.ERROR

    attach_post_verdict_artifacts(outer, [_make_artifact_row()], preserve_verdict=True)

    assert outer.status == Result.Status.ERROR


def test_artifact_row_still_reddens_an_ordinary_functional_job():
    """The negative control. On a job with no inversion a failed artifact dump
    is a genuine failure of that job and must keep reddening it: that is real
    signal on ordinary functional runs and is not suppressed here.
    """
    outer = _make_outer(
        Result.Status.OK, [_make_leaf("04700_regression_test", Result.Status.OK)]
    )

    attach_post_verdict_artifacts(outer, [_make_artifact_row()], preserve_verdict=False)

    assert outer.status == Result.Status.FAIL
    assert not outer.is_ok()


def test_artifact_rows_must_not_be_routed_through_the_inverter():
    """Executable record of why the fix pins the consumer, not the producer.

    Labelling the artifact row `LOG_CHECK` (or moving the append before the
    inverter) looks like the smaller change, but a FAIL row reaching the
    inverter is flipped to `OK` and counted as a reproduction - a broken log
    dump would become evidence the bug was validated. The label only protects
    a row that is already `OK`.
    """
    labelled_fail = _make_log_check("Scraping system tables", Result.Status.FAIL)
    outer_labelled = _make_outer(Result.Status.FAIL, [labelled_fail])
    assert invert_bugfix_validation_status(outer_labelled) is False
    assert labelled_fail.status == Result.Status.OK
    assert outer_labelled.is_success()

    plain_fail = _make_leaf("Scraping system tables", Result.Status.FAIL)
    outer_plain = _make_outer(Result.Status.FAIL, [plain_fail])
    assert invert_bugfix_validation_status(outer_plain) is False
    # Identical to the labelled arm: the label buys nothing for a FAIL row.
    assert plain_fail.status == outer_labelled.results[0].status
    assert outer_plain.status == outer_labelled.status

    labelled_ok = _make_log_check("Scraping system tables", Result.Status.OK)
    outer_ok = _make_outer(Result.Status.OK, [labelled_ok])
    assert invert_bugfix_validation_status(outer_ok) is True
    # The only case the label serves: a clean row is left alone.
    assert labelled_ok.status == Result.Status.OK


def test_collect_logs_append_preserves_the_bugfix_verdict():
    """Pin the call site: the append must go through the helper, guarded by the
    bugfix-validation label, and must stay after the inverter.

    Asserted from the source because reaching this line at runtime needs a
    server and a real log dump. Mirrors
    `test_collect_logs_gate_sees_the_pre_inversion_verdict`
    (`test_collect_core_dumps.py`), which pins the neighbouring read.
    """
    source = (JOBS_DIR / "functional_tests.py").read_text()
    tree = ast.parse(source)
    main = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )

    def calls_to(name):
        return [
            node
            for node in ast.walk(main)
            if isinstance(node, ast.Call)
            and (
                (isinstance(node.func, ast.Name) and node.func.id == name)
                or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
            )
        ]

    # The artifact rows are attached through the helper, not by a bare
    # `extend_sub_results` that would re-derive the verdict.
    attaches = calls_to("attach_post_verdict_artifacts")
    assert len(attaches) == 1, [node.lineno for node in attaches]
    guard = next(
        (kw for kw in attaches[0].keywords if kw.arg == "preserve_verdict"), None
    )
    assert guard is not None and ast.get_source_segment(source, guard.value) == (
        "is_labeled_bugfix_validation"
    ), ast.get_source_segment(source, attaches[0])

    # And it runs after the inversion, so the verdict it preserves is final.
    inversions = calls_to("invert_bugfix_validation_status")
    assert len(inversions) == 1
    assert inversions[0].lineno < attaches[0].lineno, (
        inversions[0].lineno,
        attaches[0].lineno,
    )

    # And no bare `extend_sub_results` may consume the artifact rows: that call
    # is what re-derives the parent status from them.
    for node in calls_to("extend_sub_results"):
        args = [ast.get_source_segment(source, arg) for arg in node.args]
        assert not any("extra_tests_results" in (arg or "") for arg in args), (
            node.lineno,
            args,
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
