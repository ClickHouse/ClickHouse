"""
Regression tests for the LLVM Coverage diff gate tolerance check.

Background
----------
`llvm_coverage_job.py` fails a PR when line coverage drops by more than
0.3 pp. `get_lcov_summary` parses lcov's one-decimal percentages, so the
drop was a binary float subtraction of two decimal values, and for many
pairs the result lands just above the tolerance:

    84.4 - 84.1 == 0.30000000000001137   ->  > 0.3 is True

A drop exactly equal to the tolerance therefore failed the gate, contrary
to the message it prints ("dropped 0.30 pp, tolerance 0.3 pp"). Observed on
21 of 80 `Coverage degraded` rows over 90 days, across 21 distinct PRs.

Fix
---
`coverage_drop` rounds the difference to two decimals, which is finer than
lcov's own one-decimal reporting, so no drop lcov can distinguish is masked.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.llvm_coverage_job import (
    COVERAGE_DROP_TOLERANCE,
    coverage_degraded,
    coverage_drop,
)


def _degraded(baseline: float, current: float) -> bool:
    """The gate's verdict, driving both production helpers rather than a copy."""
    return coverage_degraded(coverage_drop(baseline, current))


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
