"""
Regression tests for the LLVM Coverage diff gate tolerance check.

A drop exactly equal to the 0.3 pp tolerance must pass, as the gate's own
message states. `coverage_drop` rounds the difference so the binary-float
representation of a decimal subtraction cannot push it over the threshold.
"""

import os
import sys
import textwrap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.llvm_coverage_job import (
    COVERAGE_DROP_TOLERANCE,
    coverage_degraded,
    coverage_drop,
)

_JOB = os.path.join(os.path.dirname(__file__), "..", "jobs", "llvm_coverage_job.py")


def _degraded(baseline: float, current: float) -> bool:
    """The gate's verdict, driving both production helpers rather than a copy."""
    return coverage_degraded(coverage_drop(baseline, current))


def _gate_snippet() -> str:
    """The verdict block from llvm_coverage_job.py, verbatim.

    The gate lives inside `if __name__ == "__main__":`, so it cannot be imported;
    exec'ing its own source keeps this test honest about what the job really does.
    """
    lines = open(_JOB, encoding="utf-8").read().splitlines(True)
    start = next(i for i, l in enumerate(lines) if "_drop = coverage_drop(" in l)
    end = next(i for i, l in enumerate(lines) if "diff_res.set_failed()" in l)
    return textwrap.dedent("".join(lines[start : end + 1]))


class _ResultStub:
    """Captures the three side effects the gate has on its Result."""

    def __init__(self):
        self.info = None
        self.comment = None
        self.failed = False

    def set_comment(self, msg):
        self.comment = msg

    def set_failed(self):
        self.failed = True


def _run_gate(baseline: float, current: float) -> _ResultStub:
    """Execute the job's own verdict block and report what it did to the Result."""
    res = _ResultStub()
    ns = {
        "coverage_drop": coverage_drop,
        "coverage_degraded": coverage_degraded,
        "COVERAGE_DROP_TOLERANCE": COVERAGE_DROP_TOLERANCE,
        "b_line_cov": baseline,
        "c_line_cov": current,
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


# --- Completeness: the shard-profile presence check --------------------------
#
# A verdict may only be derived from a complete measurement. The job compares
# the profiles on disk against the expected set derived from the coverage
# artifact manifest; these tests pin the set logic the SKIPPED decision runs on.

from ci.defs.defs import LLVM_ARTIFACTS_LIST
from ci.jobs.llvm_coverage_job import (
    expected_profile_files,
    missing_profile_files,
    present_profile_files,
)


def test_expected_profiles_cover_every_coverage_artifact():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    assert len(expected) == len(LLVM_ARTIFACTS_LIST)
    assert all(f.endswith(".profdata") for f in expected)
    # One profile per artifact: a duplicate artifact name would silently weaken
    # the completeness check to fewer files than shards.
    assert len(set(expected)) == len(expected)


def test_all_profiles_present_means_nothing_missing():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    assert missing_profile_files(expected, list(expected)) == []


def test_one_absent_shard_is_reported_by_name():
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    present = [f for f in expected if f != expected[3]]
    assert missing_profile_files(expected, present) == [expected[3]]


def test_extra_files_are_not_an_error_and_do_not_mask_a_missing_shard():
    # A stale merged.profdata (or a foreign leftover) must neither redden the
    # run nor hide that an expected shard is absent.
    expected = expected_profile_files(LLVM_ARTIFACTS_LIST)
    present = [f for f in expected if f != expected[0]] + ["merged.profdata"]
    assert missing_profile_files(expected, present) == [expected[0]]


def test_present_profiles_lists_only_profdata_files(tmp_path):
    (tmp_path / "a.profdata").write_bytes(b"x")
    (tmp_path / "b.profraw").write_bytes(b"x")
    (tmp_path / "clickhouse").write_bytes(b"x")
    (tmp_path / "sub").mkdir()
    assert present_profile_files(str(tmp_path)) == ["a.profdata"]


def test_present_profiles_of_missing_directory_is_empty():
    assert present_profile_files("/nonexistent/definitely/not/here") == []
