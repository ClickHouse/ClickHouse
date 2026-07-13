#!/usr/bin/env python3
"""Focused tests for perf_api.py artifact parsing."""
from __future__ import annotations

import shutil
from pathlib import Path

import perf_api


RAW_ROWS = [
    # test name, diff, stat_threshold, changed_threshold, unstable_threshold
    ("changed_slower", 0.30, 0.05, 0.20, 0.25),
    ("boundary_equal_changed", 0.20, 0.05, 0.20, 0.25),
    ("ci_unstable_low_diff", 0.01, 0.40, 0.20, 0.25),
    ("stable_below_unstable", 0.01, 0.20, 0.20, 0.25),
    ("changed_faster", -0.30, 0.05, 0.20, 0.25),
]


def expected(diff: float, stat_threshold: float, changed_threshold: float, unstable_threshold: float) -> tuple[bool, bool, str]:
    """Replicate ci/jobs/scripts/perf/compare.sh changed_fail / unstable_fail."""
    is_changed = abs(diff) > changed_threshold and abs(diff) >= stat_threshold
    is_unstable = (not is_changed) and stat_threshold > unstable_threshold
    direction = "slowdown" if diff > 0 else ("speedup" if diff < 0 else "same")
    return is_changed, is_unstable, direction


def write_raw_fixture(path: Path) -> None:
    with path.open("w") as f:
        for i, (name, diff, stat, changed_threshold, unstable_threshold) in enumerate(RAW_ROWS):
            left = 1.0
            right = left + diff
            times_change = abs(diff) + 1.0
            fields = [
                "client_time",
                f"{left}",
                f"{right}",
                f"{diff}",
                f"{times_change}",
                f"{stat}",
                "test_a",
                str(i),
                name,
                f"{changed_threshold}",
                f"{unstable_threshold}",
            ]
            f.write("\t".join(fields) + "\n")


def test_raw_all_query_metrics_classification() -> None:
    root = Path.cwd() / "tmp" / "perf-comparison-parser-test"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    try:
        fixture = root / "all-query-metrics.tsv"
        write_raw_fixture(fixture)
        parsed = {row["queryDisplayName"]: row for row in perf_api.parse_perf_tsv([str(fixture)], metric_filter="client_time")}
        assert set(parsed) == {row[0] for row in RAW_ROWS}
        for name, diff, stat, changed_threshold, unstable_threshold in RAW_ROWS:
            exp_changed, exp_unstable, exp_direction = expected(diff, stat, changed_threshold, unstable_threshold)
            row = parsed[name]
            assert row["isChanged"] is exp_changed, (name, row)
            assert row["isUnstable"] is exp_unstable, (name, row)
            assert row["direction"] == exp_direction, (name, row)

        # Boundary/regression cases from review feedback.
        assert parsed["boundary_equal_changed"]["bucket"] == "unchanged"
        assert parsed["ci_unstable_low_diff"]["bucket"] == "unstable"
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    test_raw_all_query_metrics_classification()
    print("ok")
