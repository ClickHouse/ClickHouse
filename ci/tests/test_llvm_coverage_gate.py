"""
Regression tests for the LLVM Coverage diff gate tolerance check.

A drop exactly equal to the 0.3 pp tolerance must pass, as the gate's own
message states. `coverage_drop` rounds the difference so the binary-float
representation of a decimal subtraction cannot push it over the threshold.
"""

import ast
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.llvm_coverage_job import (
    COVERAGE_DROP_TOLERANCE,
    coverage_degraded,
    coverage_drop,
)
from ci.praktika.result import Result

_JOB = os.path.join(os.path.dirname(__file__), "..", "jobs", "llvm_coverage_job.py")


def _degraded(baseline: float, current: float) -> bool:
    """The gate's verdict, driving both production helpers rather than a copy."""
    return coverage_degraded(coverage_drop(baseline, current))


def _gate_snippet() -> str:
    """The verdict block from llvm_coverage_job.py, verbatim.

    The gate lives inside `if __name__ == "__main__":`, so it cannot be imported;
    exec'ing its own source keeps this test honest about what the job really does.

    The two statements are collected as AST NODES and re-emitted at top level.
    A line-range slice plus textwrap.dedent cannot express this: the drop
    assignment and the verdict chain sit at DIFFERENT nesting depths (the
    assignment is under `if _measurement_comparable:`, which is what keeps a
    fabricated drop from being computed for two unparsed measurements), so the
    raw slice is not a valid suite at any single indent. Extracting nodes makes
    the extraction indentation-independent, so a future re-nesting of either
    statement cannot silently degenerate this into an IndentationError.
    """
    src = open(_JOB, encoding="utf-8").read()
    tree = ast.parse(src)
    assign = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and ast.unparse(n).startswith("_drop = coverage_drop(")
    ]
    verdict = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and ast.unparse(n.test) == "not _measurement_comparable"
        and "diff_res.set_failed()" in ast.unparse(n)
    ]
    assert len(assign) == 1, f"expected one drop assignment, found {len(assign)}"
    assert len(verdict) == 1, f"expected one verdict chain, found {len(verdict)}"
    mod = ast.Module(body=[assign[0], verdict[0]], type_ignores=[])
    ast.fix_missing_locations(mod)
    return ast.unparse(mod)


class _ResultStub:
    """Captures the side effects the gate has on its Result."""

    def __init__(self):
        self.info = None
        self.comment = None
        self.failed = False
        self.status = None

    def set_comment(self, msg):
        self.comment = msg

    def set_failed(self):
        self.failed = True

    def set_status(self, status):
        self.status = status
        return self


def _run_gate(
    baseline: float, current: float, comparable: bool = True
) -> _ResultStub:
    """Execute the job's own verdict block and report what it did to the Result."""
    res = _ResultStub()
    ns = {
        "coverage_drop": coverage_drop,
        "coverage_degraded": coverage_degraded,
        "COVERAGE_DROP_TOLERANCE": COVERAGE_DROP_TOLERANCE,
        "b_line_cov": baseline,
        "c_line_cov": current,
        # Node extraction carries the whole three-arm chain, including the
        # did-not-degrade arm that the previous line-range slice stopped short
        # of. That arm reports the delta, computed here exactly as production
        # computes it, so the pass path is now executed rather than merely
        # not-failed.
        "delta": current - baseline,
        # The gate only reaches a tolerance verdict for two comparable
        # measurements; these tests are about the tolerance, so they say so.
        "_measurement_comparable": comparable,
        "_incomparable_reason": "" if comparable else "test-supplied reason",
        "Result": Result,
        "diff_res": res,
        "print": lambda *a, **k: None,
    }
    exec(_gate_snippet(), ns)  # noqa: S102 - trusted first-party source
    return res


def test_gate_snippet_is_the_real_verdict_block():
    # Without this the extraction could silently degenerate and make every
    # _run_gate assertion below vacuous.
    src = _gate_snippet()
    assert "coverage_drop(" in src
    assert "coverage_degraded(" in src
    assert "diff_res.set_failed()" in src
    # The verdict is reached only for two comparable measurements; if this guard
    # ever leaves the block, the comparable=False assertions below go vacuous.
    assert "_measurement_comparable" in src
    # All THREE arms, so the pass path below is executed rather than merely
    # not-failed. The previous line-range extraction stopped at set_failed() and
    # so never carried this arm.
    assert "did not degrade beyond tolerance" in src


def test_tolerance_is_unchanged():
    assert COVERAGE_DROP_TOLERANCE == 0.3


def test_drop_equal_to_tolerance_passes():
    # The only two value pairs behind all 21 observed failures.
    assert not _degraded(84.4, 84.1)
    assert not _degraded(85.4, 85.1)


def test_old_expression_did_fire_on_those_pairs():
    # Without this the suite cannot tell the fixed and broken versions apart.
    assert 84.4 - 84.1 > COVERAGE_DROP_TOLERANCE
    assert 85.4 - 85.1 > COVERAGE_DROP_TOLERANCE


def test_drop_above_tolerance_still_fails():
    assert _degraded(86.30, 85.99)
    assert _degraded(86.3, 85.8)


def test_large_drop_still_fails():
    # The shape reported on PR #105684; the gate must not be disabled.
    assert _degraded(86.20, 28.60)


def test_coverage_increase_passes():
    assert not _degraded(86.53, 86.54)


def test_contract_over_full_range():
    # For every one-decimal baseline, the verdict must be `drop > 0.3`.
    mismatches = []
    for step in range(0, 1001):
        baseline = step / 10.0
        for drop in ("0.29", "0.30", "0.31", "0.35", "0.40"):
            current = round(baseline - float(drop), 2)
            if current < 0:
                continue
            expected = float(drop) > COVERAGE_DROP_TOLERANCE
            if _degraded(baseline, current) != expected:
                mismatches.append((baseline, current, drop))
    assert mismatches == [], f"{len(mismatches)} mismatches, first: {mismatches[:5]}"


def test_message_reports_the_value_it_compared():
    baseline, current = 86.30, 85.99
    drop = coverage_drop(baseline, current)
    assert _degraded(baseline, current)
    # The gate interpolates this same value, so the printed number cannot
    # disagree with the number judged.
    assert f"{drop:.2f}" == "0.31"


def test_gate_passes_a_drop_equal_to_tolerance():
    # Drives the production call site, not just the helpers it wires together.
    assert _run_gate(84.4, 84.1).failed is False
    assert _run_gate(85.4, 85.1).failed is False


def test_gate_fails_a_drop_above_tolerance_with_the_value_it_judged():
    res = _run_gate(86.30, 85.99)
    assert res.failed is True
    assert res.comment == (
        "Coverage degraded: master 86.30% \u2192 PR 85.99%"
        " (dropped 0.31 pp, tolerance 0.3 pp)"
    )
    assert res.info == res.comment


def test_gate_still_fails_the_large_drop():
    res = _run_gate(86.20, 28.60)
    assert res.failed is True
    assert "dropped 57.60 pp" in res.comment


def test_gate_produces_no_verdict_for_two_incomparable_measurements():
    # The same over-tolerance drop that fails above must NOT fail when the two
    # measurements are not comparable: the number itself is then meaningless.
    res = _run_gate(86.20, 28.60, comparable=False)
    assert res.failed is False
    assert res.status == Result.Status.SKIPPED
    assert "test-supplied reason" in res.comment
