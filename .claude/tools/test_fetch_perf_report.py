#!/usr/bin/env python3
"""Tests for the query classification in fetch_perf_report.py.

These prove that the helper classifies queries with the same effective
per-query thresholds as ci/jobs/scripts/perf/compare.sh, including the cases
where the historical / per-test thresholds raise changed_threshold or
unstable_threshold above the 0.15 / 0.25 floors. In those cases classifying
with the floor constants alone would produce false "changed" / "unstable"
findings that CI treats as noise.

The test runs clickhouse-local through the same SQL builders the tool uses, so
it exercises the real classification logic. It is skipped when the clickhouse
binary is not available.

Run directly:  python3 .claude/tools/test_fetch_perf_report.py
"""

import importlib.util
import os
import shutil
import tempfile
import types

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "fetch_perf_report", os.path.join(_HERE, "fetch_perf_report.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fpr = _load_module()


def _args(**overrides):
    defaults = dict(
        metric="client_time",
        arch="all",
        shard=None,
        test=None,
        query=None,
        sort="diff",
        show_all=True,
    )
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


# Each row mirrors one all-query-metrics.tsv record (after the arch/shard_num
# columns are prepended by download_shard):
#   arch shard metric left right diff times stat test qidx qname c_thr u_thr
# The interesting rows are "noise_below_raised_changed" and
# "stable_below_raised_unstable": with floor-only logic they would be flagged,
# but their per-query thresholds are above the floor, so CI ignores them.
ROWS = [
    # name, diff, stat_threshold, changed_threshold, unstable_threshold
    ("changed_slower", 0.30, 0.05, 0.20, 0.25),
    ("noise_below_raised_changed", 0.18, 0.05, 0.20, 0.25),
    ("unstable", 0.05, 0.30, 0.20, 0.25),
    ("stable_below_raised_unstable", 0.05, 0.28, 0.20, 0.30),
    ("changed_faster", -0.30, 0.05, 0.20, 0.25),
]


def _expected(diff, stat, changed_thr, unstable_thr):
    """Replicate the compare.sh changed_fail / unstable_fail classification."""
    is_changed = abs(diff) > changed_thr and abs(diff) >= stat
    is_unstable = (not is_changed) and stat > unstable_thr
    direction = "slower" if diff > 0 else ("faster" if diff < 0 else "same")
    return is_changed, is_unstable, direction


def _write_fixture(path):
    with open(path, "w") as f:
        for i, (name, diff, stat, c_thr, u_thr) in enumerate(ROWS):
            left, right = 1.0, 1.0 + diff
            times = abs(diff) + 1.0
            fields = [
                "amd", "1", "client_time",
                f"{left}", f"{right}", f"{diff}", f"{times}", f"{stat}",
                "test_a", str(i), name, f"{c_thr}", f"{u_thr}",
            ]
            f.write("\t".join(fields) + "\n")


def test_classification_matches_compare_sh():
    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    tmpdir = tempfile.mkdtemp(prefix="test_perf_report_")
    try:
        data_path = os.path.join(tmpdir, "all.tsv")
        _write_fixture(data_path)

        assert fpr.count_tsv_columns(data_path) == fpr.COLUMNS_WITH_THRESHOLDS

        args = _args()
        sql = fpr.build_detail_sql(args, data_path, has_thresholds=True)
        rows = {r["query"]: r for r in fpr.parse_jsonl(fpr.run_ch(sql))}

        assert len(rows) == len(ROWS), rows

        for name, diff, stat, c_thr, u_thr in ROWS:
            exp_changed, exp_unstable, exp_dir = _expected(diff, stat, c_thr, u_thr)
            row = rows[name]
            assert bool(row["is_changed"]) == exp_changed, (name, row)
            assert bool(row["is_unstable"]) == exp_unstable, (name, row)
            assert row["direction"] == exp_dir, (name, row)

        # The two rows whose thresholds were raised above the floors must NOT be
        # flagged - this is exactly what the old floor-only logic got wrong.
        assert not rows["noise_below_raised_changed"]["is_changed"]
        assert not rows["noise_below_raised_changed"]["is_unstable"]
        assert not rows["stable_below_raised_unstable"]["is_unstable"]
        assert not rows["stable_below_raised_unstable"]["is_changed"]

        # Sanity: floor-only classification (has_thresholds=False) WOULD flag
        # them, which is the regression this change fixes.
        floor_sql = fpr.build_detail_sql(args, data_path, has_thresholds=False)
        floor_rows = {r["query"]: r for r in fpr.parse_jsonl(fpr.run_ch(floor_sql))}
        assert floor_rows["noise_below_raised_changed"]["is_changed"]
        assert floor_rows["stable_below_raised_unstable"]["is_unstable"]
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_summary_counts():
    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    tmpdir = tempfile.mkdtemp(prefix="test_perf_report_")
    try:
        data_path = os.path.join(tmpdir, "all.tsv")
        _write_fixture(data_path)
        shard_meta = [{"name": "amd 1/1", "arch": "amd", "shard_num": 1}]
        sql = fpr.build_summary_sql(_args(), shard_meta, data_path, has_thresholds=True)
        summary = fpr.parse_jsonl(fpr.run_ch(sql))
        assert len(summary) == 1, summary
        s = summary[0]
        # changed_slower + changed_faster = 1 slower + 1 faster; 1 unstable.
        assert s["faster"] == 1, s
        assert s["slower"] == 1, s
        assert s["unstable"] == 1, s
        assert s["total"] == len(ROWS), s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_classification_matches_compare_sh()
    test_summary_counts()
    print("All fetch_perf_report tests passed (or skipped).")
