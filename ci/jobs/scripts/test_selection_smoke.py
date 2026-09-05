"""Deterministic selection smoke; live monitoring is opt-in and CI-only."""

import argparse
import json
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from ci.jobs.scripts.coverage_selection import (
    build_candidate_query,
    canonical_coverage_paths,
    protect_selection,
    rank_candidates,
    validate_snapshots,
)
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG

FIXTURE_TIME = "2026-09-04 03:00:00"
FIXTURE_DIFF = """--- a/src/Interpreters/Fixture.cpp
+++ b/src/Interpreters/Fixture.cpp
@@ -10,3 +10,3 @@
-old
+new
 context
 context
"""


def fixture_snapshots():
    return [
        {
            "check_start_time": FIXTURE_TIME,
            "check_name": f"Stateless tests (amd_llvm_coverage_per_test, per_test_coverage, {shard}/8)",
            "exported_tests": 200,
        }
        for shard in range(1, 9)
    ]


def fixture_region(path="src/Interpreters/Fixture.cpp", tests=None):
    tests = tests or [("00001_select_1.sql", 86)]
    return {
        "file": path,
        "line_start": 10,
        "line_end": 10,
        "region_owners": len(tests),
        "observations": [
            [test, FIXTURE_TIME, fixture_snapshots()[0]["check_name"], entry_count]
            for test, entry_count in tests
        ],
    }


class FixtureCIDB:
    def __init__(self, path):
        self.path = path
        self.queries = []

    def query(self, query, **kwargs):
        self.queries.append(query)
        if "AS exported_tests" in query:
            return "\n".join(map(json.dumps, fixture_snapshots()))
        if "LIMIT 100" in query:
            return json.dumps({"file": self.path, "line_start": 10, "line_end": 10})
        if "WITH per_run_region_test" not in query:
            raise AssertionError(f"Unexpected selection query: {query}")
        # Model the stored-path predicate, so a regression to dotted-only SQL
        # loses the current fixture rather than passing a canned response.
        if repr(self.path) not in query:
            return ""
        return json.dumps(fixture_region(self.path))


class SelectionSmoke(unittest.TestCase):
    def test_deleted_and_renamed_source_uses_coverage_coordinates(self):
        diff = """--- a/src/old.cpp
+++ b/src/new.cpp
@@ -10,1 +10,1 @@
-old
+new
--- a/src/deleted.cpp
+++ /dev/null
@@ -20,1 +0,0 @@
-deleted
"""
        self.assertEqual(
            Targeting._parse_diff_lines(diff),
            [("src/deleted.cpp", 20), ("src/old.cpp", 10), ("src/old.cpp", 11)],
        )
        self.assertEqual(
            Targeting._parse_diff_hunk_ranges(diff),
            {"src/old.cpp": [(10, 10)], "src/deleted.cpp": [(20, 20)]},
        )

    def test_ci_diff_is_pinned_to_manifest_sha(self):
        target = Targeting(
            SimpleNamespace(
                job_name="Stateless tests",
                pr_number=1,
                repo_name="ClickHouse/ClickHouse",
                is_local_run=False,
                sha="head",
            )
        )
        metadata = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"head": {"sha": "head"}, "base": {"sha": "base"}},
        )
        diff = SimpleNamespace(raise_for_status=lambda: None, text=FIXTURE_DIFF)
        with patch("requests.get", side_effect=[metadata, diff]) as get:
            self.assertEqual(target.get_diff_text(), FIXTURE_DIFF)
        self.assertTrue(get.call_args.args[0].endswith("/compare/base...head"))

    def test_production_path_contract(self):
        for path in canonical_coverage_paths("src/Interpreters/Fixture.cpp"):
            with self.subTest(path=path):
                target = Targeting(
                    SimpleNamespace(job_name="Stateless tests", pr_number=1)
                )
                target._cidb = FixtureCIDB(path)
                target._coverage_snapshots = fixture_snapshots()
                target._diff_text = FIXTURE_DIFF
                with patch.object(
                    target, "get_previously_failed_tests", return_value=[]
                ), patch.object(
                    target,
                    "get_changed_or_new_tests_with_info",
                    return_value=([], None),
                ):
                    tests, _ = target.get_all_relevant_tests_with_info()
                self.assertEqual(tests, ["00001_select_1."])
                self.assertEqual(
                    target.selection_diagnostics["selected"][0]["source"],
                    "primary_coverage",
                )
                self.assertEqual(target.selection_diagnostics["canary"]["status"], "OK")
                self.assertTrue(
                    all("keyword" not in query for query in target._cidb.queries)
                )

    def test_path_rejection(self):
        for path in (
            "/build/src/a.cpp",
            "../src/a.cpp",
            "src/../a.cpp",
            "C:/src/a.cpp",
            "",
        ):
            with self.assertRaises(ValueError):
                canonical_coverage_paths(path)

    def test_snapshot_health_and_cutoff(self):
        validate_snapshots(fixture_snapshots(), "2026-09-05 00:00:00")
        for snapshots, cutoff in (
            (fixture_snapshots()[:7], "2026-09-05 00:00:00"),
            (fixture_snapshots(), "2026-09-09 00:00:00"),
            (fixture_snapshots(), "2026-09-03 00:00:00"),
        ):
            with self.assertRaises(ValueError):
                validate_snapshots(snapshots, cutoff)

    def test_failing_canary_propagates(self):
        target = Targeting(SimpleNamespace(job_name="Stateless tests"))
        target._coverage_snapshots = fixture_snapshots()
        target._cidb = FixtureCIDB("/build/src/Interpreters/Fixture.cpp")
        with self.assertRaises(ValueError):
            target.check_coverage_canary()

    def test_entry_count_does_not_demote_strong_coverage(self):
        # PR #117331 exposed the old global tier: entry count 86 must not
        # move stronger narrow evidence behind low-count infrastructure hits.
        strong = fixture_region(tests=[("relevant", 86)])
        weak = fixture_region(tests=[("weak", 1)])
        weak.update({"line_start": 11, "line_end": 20})
        candidates = rank_candidates(
            [strong, weak],
            [(strong["file"], 10)],
            {strong["file"]: [(10, 20)]},
            fixture_snapshots(),
        )
        self.assertEqual(candidates[0]["test"], "relevant")

    def test_observations_do_not_multiply_score(self):
        region = fixture_region()
        changed = [(region["file"], 10)]
        initial = rank_candidates([region], changed, {}, fixture_snapshots())[0][
            "score"
        ]
        region["observations"].append(
            [
                "00001_select_1.sql",
                FIXTURE_TIME,
                fixture_snapshots()[1]["check_name"],
                254,
            ]
        )
        self.assertEqual(
            rank_candidates([region], changed, {}, fixture_snapshots())[0]["score"],
            initial,
        )

    def test_region_relative_bonus_is_bounded(self):
        region = fixture_region(tests=[("low", 1), ("high", 254), ("unknown", 255)])
        changed = [(region["file"], 10)]
        base = {
            c["test"]: c["score"]
            for c in rank_candidates([region], changed, {}, fixture_snapshots())
        }
        for mode in ("relative-low", "relative-high"):
            for candidate in rank_candidates(
                [region], changed, {}, fixture_snapshots(), entry_mode=mode
            ):
                ratio = candidate["score"] / base[candidate["test"]]
                self.assertGreaterEqual(ratio, 0.9)
                self.assertLessEqual(ratio, 1.1)
                if candidate["test"] == "unknown":
                    self.assertEqual(ratio, 1)

    def test_out_of_snapshot_row_rejected(self):
        region = fixture_region()
        region["observations"][0][1] = "2026-09-06 00:00:00"
        with self.assertRaises(ValueError):
            rank_candidates([region], [(region["file"], 10)], {}, fixture_snapshots())

    def test_protected_order_and_overflow(self):
        candidates = [
            {
                "test": "coverage",
                "score": 1,
                "features": [],
                "source": "primary_coverage",
            }
        ]
        config = replace(SELECTION_CONFIG, max_selected_tests_temporary=2)
        result = protect_selection(
            ["changed"], ["failed", "changed"], candidates, str, config
        )
        self.assertEqual([r["test"] for r in result["selected"]], ["changed", "failed"])
        self.assertEqual(
            result["selected"][0]["sources"], ["changed", "previously_failed"]
        )
        self.assertTrue(result["ceiling_truncated"])
        result = protect_selection(["a", "b", "c"], [], candidates, str, config)
        self.assertEqual(result["mandatory_overflow"], 1)
        self.assertEqual(result["selected_count"], 3)

    def test_query_keeps_file_pruning_and_separate_hunks(self):
        query = build_candidate_query(
            [("src/a.cpp", 10), ("src/a.cpp", 100)], {}, fixture_snapshots()
        )
        self.assertIn("file IN ('src/a.cpp', './src/a.cpp')", query)
        self.assertIn("line_end >= 10 AND line_start <= 10", query)
        self.assertNotIn("line_end >= 10 AND line_start <= 100", query)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    parser.add_argument(
        "--url", help="Explicit read-only coverage endpoint for operational smoke"
    )
    args = parser.parse_args()
    if args.live:
        from ci.praktika.cidb import CIDB
        from ci.praktika.info import Info

        target = Targeting(Info())
        target.job_type = Targeting.STATELESS_JOB_TYPE
        if args.url:
            target._cidb = CIDB(args.url)
        try:
            target.check_coverage_canary()
        finally:
            print(json.dumps(target.selection_diagnostics, indent=2))
    else:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelectionSmoke)
        if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
            raise SystemExit(1)


if __name__ == "__main__":
    main()
