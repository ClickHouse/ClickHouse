"""
Tests for `perf_dashboard_gate` (ci/jobs/performance_tests.py).

A dashboard `confirmed_regression` fails a `master_head` shard only when the
shard's own report published the same (test, query_index) as slower, and the
shard's report is read from `report/all-query-metrics.tsv`, whose column
positions the predicate depends on.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import performance_tests as pt

ARCH = "arm"


class _Info:
    pr_number = 1
    sha = "0123456789ab"
    is_local_run = False


def _metrics_row(test, query_index, diff, stat_threshold, changed_threshold):
    """One `client_time` row of `report/all-query-metrics.tsv`."""
    return "\t".join(
        [
            pt.DASHBOARD_GATE_METRIC,
            "1.0",
            "2.0",
            str(diff),
            "2.000",
            str(stat_threshold),
            test,
            str(query_index),
            f"SELECT {query_index}",
            str(changed_threshold),
            "0.25",
        ]
    )


def _dashboard_slowdown(test, query_index, tier="confirmed_regression"):
    return {
        "arch": ARCH,
        "metric": pt.DASHBOARD_GATE_METRIC,
        "test": test,
        "queryIndex": query_index,
        "oldValue": 1.0,
        "newValue": 2.0,
        "diffPercent": 1.0,
        "confidence": {"tier": tier, "reason": "stub"},
    }


class PerfDashboardGateTest(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp_dir.cleanup)
        self.addCleanup(setattr, pt, "dashboard_api_get", pt.dashboard_api_get)

    def metrics_file(self, *rows):
        fd, path = tempfile.mkstemp(suffix=".tsv", dir=self.tmp_dir.name)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write("".join(f"{row}\n" for row in rows))
        return path

    def stub_dashboard(self, slowdowns):
        def get(path, params=None, timeout_sec=60):
            if path.endswith("/confidence"):
                return {"slowdowns": slowdowns}
            return {"testTimes": [{"title": f"Test times, {ARCH} release"}]}

        pt.dashboard_api_get = get

    def gate(self, metrics_path):
        return pt.perf_dashboard_gate(_Info(), ARCH, metrics_path)

    def test_only_reported_slowdowns_are_confirmed(self):
        path = self.metrics_file(
            _metrics_row("over_both", 1, 0.30, 0.20, 0.15),
            # `changed_show` only: within 0.05 of the threshold but under it.
            _metrics_row("under_changed", 2, 0.12, 0.10, 0.15),
            _metrics_row("under_stat", 3, 0.30, 0.40, 0.15),
            _metrics_row("faster", 4, -0.30, 0.20, 0.15),
        )
        tests, slowdowns = pt.read_shard_confirmed_slowdowns(path)
        self.assertEqual(
            tests, ["over_both", "under_changed", "under_stat", "faster"]
        )
        self.assertEqual(slowdowns, {("over_both", 1)})

    def test_no_reported_slowdown_skips_the_dashboard(self):
        def explode(*args, **kwargs):
            raise AssertionError("the dashboard must not be asked")

        pt.dashboard_api_get = explode
        path = self.metrics_file(_metrics_row("under_changed", 2, 0.12, 0.10, 0.15))
        self.assertEqual(self.gate(path), [])

    def test_malformed_row_fails_closed(self):
        truncated = "\t".join(
            _metrics_row("tpcds", 71, 22.25, 22.25, 0.15).split("\t")[:9]
        )
        path = self.metrics_file(truncated)
        with self.assertRaises(pt.PerfDashboardError) as raised:
            pt.read_shard_confirmed_slowdowns(path)
        self.assertIn(path, str(raised.exception))
        self.assertIn("tpcds #71", str(raised.exception))

    def test_agreement_blocks(self):
        self.stub_dashboard([_dashboard_slowdown("tpcds", 71)])
        path = self.metrics_file(_metrics_row("tpcds", 71, 22.25, 22.25, 0.15))
        self.assertEqual(
            [(row["test"], row["query_index"]) for row in self.gate(path)],
            [("tpcds", 71)],
        )

    def test_disagreement_passes(self):
        self.stub_dashboard([_dashboard_slowdown("tpcds", 71)])
        # The shard reported a slowdown, but of another query.
        other_query = self.metrics_file(_metrics_row("tpcds", 58, 0.90, 0.20, 0.15))
        self.assertEqual(self.gate(other_query), [])
        # The same query as `test_agreement_blocks`, under its own threshold.
        under_threshold = self.metrics_file(
            _metrics_row("tpcds", 71, 6.34, 6.34, 10.72)
        )
        self.assertEqual(self.gate(under_threshold), [])


if __name__ == "__main__":
    unittest.main()
