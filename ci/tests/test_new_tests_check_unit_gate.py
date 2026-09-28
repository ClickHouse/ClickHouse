"""
Tests for the unit-test branch of the bug-fix gate in
`ci.jobs.scripts.workflow_hooks.new_tests_check`.

The unit-test Bugfix Validation job is `allow_failure`, so the merge decision for a
unit-only bug-fix PR is made here: block iff that job definitively FAILED to reproduce
(the added test passes on the merge-base too). A reproduction (OK/XFAIL), an inconclusive
ERROR (the before-binary could not be compiled or crashed before any test), or a skip
must NOT block — "we couldn't determine" is not grounds to block merge.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.defs.defs import JobNames
from ci.jobs.scripts.workflow_hooks import new_tests_check as ntc
from ci.praktika.result import Result


class _FakeWorkflowResult:
    def __init__(self, ut_status):
        self.results = []
        if ut_status is not None:
            self.results.append(
                Result(name=JobNames.BUGFIX_VALIDATE_UT, status=ut_status)
            )


class _FakeInfo:
    workflow_name = "PR"


def _patch(monkeypatch, ut_status):
    monkeypatch.setattr(ntc, "Info", lambda: _FakeInfo())
    monkeypatch.setattr(
        ntc.Result, "from_fs", staticmethod(lambda name: _FakeWorkflowResult(ut_status))
    )


def test_refuted_only_when_job_failed(monkeypatch):
    _patch(monkeypatch, Result.Status.FAIL)
    assert ntc.unit_bugfix_validation_refuted() is True


@pytest.mark.parametrize(
    "status",
    [
        Result.Status.OK,       # reproduced, or skipped (nothing to validate)
        Result.Status.ERROR,    # inconclusive — before-binary couldn't compile / crashed
        Result.Status.SKIPPED,  # job filtered out
    ],
)
def test_not_refuted_for_non_fail(monkeypatch, status):
    _patch(monkeypatch, status)
    assert ntc.unit_bugfix_validation_refuted() is False


def test_not_refuted_when_job_absent(monkeypatch):
    # No unit Bugfix Validation job in the workflow result -> do not block.
    _patch(monkeypatch, None)
    assert ntc.unit_bugfix_validation_refuted() is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
