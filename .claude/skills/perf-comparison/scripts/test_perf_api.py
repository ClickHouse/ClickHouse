#!/usr/bin/env python3
"""Focused tests for perf_api.py artifact parsing."""
from __future__ import annotations

import argparse
import contextlib
import gzip
import io
import shutil
import subprocess
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


def test_reads_compressed_all_query_metrics() -> None:
    """iter_tsv_dicts must parse zstd/gzip artifacts identically to plain (CI compresses
    text artifacts over a size threshold, see ci/praktika/s3.py)."""
    root = Path.cwd() / "tmp" / "perf-comparison-compressed-test"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    try:
        plain = root / "all-query-metrics.tsv"
        write_raw_fixture(plain)
        expected_rows = perf_api.iter_tsv_dicts(str(plain))

        raw = plain.read_bytes()
        (root / "gz.tsv").write_bytes(gzip.compress(raw))
        subprocess.run(["zstd", "-q", "-f", str(plain), "-o", str(root / "zst.tsv")], check=True)

        for variant in ("gz.tsv", "zst.tsv"):
            assert perf_api.iter_tsv_dicts(str(root / variant)) == expected_rows, variant
    finally:
        shutil.rmtree(root, ignore_errors=True)


def stdout_of(func, **fields) -> str:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        func(argparse.Namespace(**fields))
    return out.getvalue()


def test_not_judged_rows_are_not_reported_as_unchanged() -> None:
    """A shard whose learned thresholds could not be fetched is not judged at all: its
    named rows carry not_judged=1, its raw rows carry infinite bars. is_changed=0 on such
    a row is the absence of a verdict, so it must not be counted as an unchanged query."""
    root = Path.cwd() / "tmp" / "perf-comparison-not-judged-test"
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    try:
        header = ["metric", "arch", "shard", "old", "new", "diff", "times_change", "stat_threshold",
                  "test", "query_index", "is_changed", "is_unstable", "direction", "query", "not_judged"]
        named = root / "named.tsv"
        named.write_text("\n".join("\t".join(row) for row in [
            header,
            ["client_time", "arm", "1", "1.0", "2.0", "1.0", "2.0", "0.05", "t", "4", "0", "0", "slowdown", "abstained", "1"],
            ["client_time", "amd", "2", "1.0", "1.3", "0.3", "1.3", "0.05", "t", "0", "1", "0", "slowdown", "judged", "0"],
        ]) + "\n")
        parsed = {row["queryDisplayName"]: row for row in perf_api.parse_perf_tsv([str(named)])}
        assert parsed["abstained"]["notJudged"] is True, parsed
        assert parsed["abstained"]["bucket"] == "not-judged", parsed
        assert parsed["abstained"]["isChanged"] is False, parsed
        assert parsed["judged"]["notJudged"] is False and parsed["judged"]["bucket"] == "changed", parsed

        # Raw rows have no marker column; both bars infinite is the sentinel. One
        # infinite bar is not: eqmed.sql divides by the baseline median, so a zero
        # baseline gives an infinite diff, which raises changed_threshold alone
        # (the stat_threshold quantile excludes those rows). Such a row keeps its
        # own verdict, and so does a NaN one.
        raw = root / "all-query-metrics.tsv"
        raw.write_text(
            "client_time\t1.0\t2.0\t1.0\t2.0\t0.05\tt\t4\tabstained\tinf\tinf\n"
            "client_time\t1.0\t1.0\t0.0\t1.0\t0.40\tt\t5\tone_bar_inf\tinf\t0.25\n"
            "client_time\t1.0\t1.0\t0.0\t1.0\t0.05\tt\t6\tnan_bars\tnan\tnan\n")
        raw_rows = {row["queryDisplayName"]: row for row in perf_api.parse_perf_tsv([str(raw)])}
        assert raw_rows["abstained"]["bucket"] == "not-judged", raw_rows
        assert raw_rows["one_bar_inf"]["bucket"] == "unstable", raw_rows
        assert raw_rows["nan_bars"]["notJudged"] is False, raw_rows

        # Control: a fixture with finite bars must mark nothing, so the assertions above
        # cannot pass on a parser that labels every row.
        finite = root / "finite.tsv"
        write_raw_fixture(finite)
        assert not [row for row in perf_api.parse_perf_tsv([str(finite)]) if row["notJudged"]]

        # All three disclosure sites of the inventory, each asserted on its own: the row is
        # dropped from every judged table, so only these say the shard was not judged.
        inventory = stdout_of(perf_api.cmd_tsv_inventory, tsv=[str(named)], metric="client_time",
                              arch="", limit=5, show_all=False)
        assert "**1 of 2 rows come from a shard CI did not judge**" in inventory, inventory
        assert "- Rows CI did not judge: **1**" in inventory, inventory
        assert "Rows CI did not judge (no verdict" in inventory, inventory
        control = stdout_of(perf_api.cmd_tsv_inventory, tsv=[str(finite)], metric="client_time",
                            arch="", limit=5, show_all=False)
        assert "did not judge: **0**" in control and "come from a shard" not in control, control

        # Only abstained rows: master-checks classifies nothing, so the reason has to be
        # printed or the run reads as clean. Returns before any network call.
        checks = stdout_of(perf_api.cmd_master_checks, tsv=[str(raw)], pr=None, metric="client_time",
                           arch="", limit=None, days=30, all_runs=False, base=perf_api.BASE_DEFAULT,
                           metrics=perf_api.DEFAULT_METRICS, play_url=perf_api.PLAY_DEFAULT, play_user="explorer")
        assert "carry no verdict" in checks, checks
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    test_raw_all_query_metrics_classification()
    test_reads_compressed_all_query_metrics()
    test_not_judged_rows_are_not_reported_as_unchanged()
    print("ok")
