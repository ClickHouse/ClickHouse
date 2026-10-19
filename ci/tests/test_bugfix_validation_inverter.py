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

See ClickHouse/ClickHouse#105789 and #103541.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.functional_tests import invert_bugfix_validation_status
from ci.praktika.result import Result


def _make_leaf(name, status, info=""):
    return Result(name=name, status=status, info=info)


def _make_log_check(name, status):
    """A server-log / runner health-check row, as produced by
    `check_fatal_messages_in_logs` and labelled `LOG_CHECK`."""
    leaf = Result(name=name, status=status)
    leaf.set_label(Result.Label.LOG_CHECK)
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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
