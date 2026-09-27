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
    load_snapshots,
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
        if "from checks\n" in query:
            return ""
        if "SELECT DISTINCT check_start_time" in query:
            return json.dumps({"check_start_time": FIXTURE_TIME})
        if "AS exported_tests" in query:
            return "\n".join(map(json.dumps, fixture_snapshots()))
        if "LIMIT 1 FORMAT JSONEachRow" in query:
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

    def test_cutoff_is_pinned_per_attempt(self):
        def target(**kwargs):
            return Targeting(
                SimpleNamespace(
                    job_name="Stateless tests",
                    pr_number=1,
                    repo_name="ClickHouse/ClickHouse",
                    is_local_run=False,
                    run_id=7,
                    **kwargs,
                )
            )

        # 2026-09-05 00:00:00 UTC, resolved once by the config job.
        first = target(run_attempt=1, workflow_start_time=1788566400.0)
        with patch("requests.get") as get:
            self.assertEqual(first.selection_cutoff(), "2026-09-05 00:00:00")
        get.assert_not_called()

        rerun = target(run_attempt=2, workflow_start_time=1788566400.0)
        response = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"run_started_at": "2026-09-06T10:20:30Z"},
        )
        with patch("requests.get", return_value=response) as get:
            self.assertEqual(rerun.selection_cutoff(), "2026-09-06 10:20:30")
        self.assertTrue(get.call_args.args[0].endswith("/actions/runs/7/attempts/2"))

        # Both the failures and the coverage snapshots are read up to the cutoff.
        first._cidb = FixtureCIDB("src/Interpreters/Fixture.cpp")
        first._test_exists = lambda test: True
        first.get_previously_failed_tests()
        first.coverage_snapshots()
        failed_query, times_query, health_query = first._cidb.queries
        self.assertIn("check_start_time < toDateTime('2026-09-05 00:00:00', 'UTC')", failed_query)
        self.assertNotIn("now()", failed_query)
        self.assertIn("toDateTime('2026-09-04 23:00:00', 'UTC')", times_query)
        self.assertIn(f"toDateTime('{FIXTURE_TIME}', 'UTC')", health_query)

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

    def test_snapshots_read_only_needed_exports(self):
        shards = [
            f"Stateless tests (amd_llvm_coverage_per_test, per_test_coverage, {shard}/8)"
            for shard in range(1, 9)
        ]
        # One export per shard per day, the newest one unhealthy for shard 1.
        days = [f"2026-09-{day:02d} 03:00:00" for day in range(1, 11)]
        queries = []

        def query(sql, timeout):
            queries.append(sql)
            if "SELECT DISTINCT check_start_time" in sql:
                return "\n".join(json.dumps({"check_start_time": t}) for t in days)
            rows = [
                {"check_start_time": t, "check_name": name, "exported_tests": 200}
                for t in sorted(days, reverse=True)
                if f"'{t}'" in sql
                for name in shards
                if not (t == days[-1] and name == shards[0])
            ]
            return "\n".join(map(json.dumps, rows))

        snapshots = load_snapshots(query, "2026-09-11 00:00:00")
        self.assertEqual(len(snapshots), 3 * 8)
        by_shard = {}
        for row in snapshots:
            by_shard.setdefault(row["check_name"], []).append(row["check_start_time"])
        self.assertEqual(by_shard[shards[0]], days[-4:-1][::-1])
        self.assertEqual(by_shard[shards[1]], days[-3:][::-1])
        # The oldest days are never read.
        self.assertTrue(all(f"'{days[0]}'" not in sql for sql in queries[1:]))
        self.assertTrue(all("use_query_cache = 1" in sql for sql in queries))
        validate_snapshots(snapshots, "2026-09-11 00:00:00")

    def test_cidb_timeout_is_distinguishable(self):
        # The targeted job skips instead of failing only on this exception.
        import requests

        from ci.praktika.cidb import CIDB, CIDBTimeoutError

        cidb = CIDB(url="http://cidb.invalid", user="", passwd="")
        with patch("requests.post", side_effect=requests.exceptions.ReadTimeout("slow")):
            with self.assertRaises(CIDBTimeoutError):
                cidb.query("SELECT 1", retries=1)
        with patch("requests.post", side_effect=requests.exceptions.ConnectionError("down")):
            with self.assertRaises(RuntimeError) as error:
                cidb.query("SELECT 1", retries=1)
            self.assertNotIsInstance(error.exception, CIDBTimeoutError)

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

    def test_tag_edit_sections(self):
        def edit(ext, body, checked_out=None, header="index 1..2 100644\n"):
            path = f"tests/queries/0_stateless/00001_x.{ext}"
            section = f"diff --git a/{path} b/{path}\n{header}--- a/{path}\n+++ b/{path}\n"
            if checked_out is None:  # the new side of a hunk spanning the whole file
                checked_out = "".join(f"{x[1:]}\n" for x in body.splitlines() if x[:1] != "-")
            result = Targeting._tag_edit(f"{section}@@ -1 +1 @@\n{body}", lambda _: checked_out)
            self.assertIn(result and result[0], (None, path))
            return result and result[1]

        tag_sh = " #!/usr/bin/env bash\n-# Tags: race\n+# Tags: race, no-msan\n"
        for name, ext, body, expected in (
            ("modify", "sh", tag_sh, {"no-msan"}),
            ("add with blank", "sql", "+-- Tags: no-msan\n+\n SELECT 1;\n", {"no-msan"}),
            ("remove with blank", "sql", "--- Tags: no-msan\n-\n SELECT 1;\n", {"no-msan"}),
            ("code", "sql", "--- Tags: race\n+-- Tags: no-msan\n-SELECT 1;\n+SELECT 2;\n", None),
            ("second run", "sql", "--- Tags: race\n+-- Tags: no-msan\n SELECT 1;\n+\n", None),
            ("heredoc", "sh", " cat <<EOF\n-# Tags: race\n+# Tags: race, no-msan\n EOF\n", None),
            ("below code", "sql", " SET x = 1;\n--- Tags: race\n+-- Tags: no-msan\n", None),
            ("reference", "reference", tag_sh, None),
        ):
            with self.subTest(name):
                self.assertEqual(edit(ext, body), expected)
        with self.subTest("stale checkout"):
            self.assertIsNone(edit("sh", tag_sh, checked_out="# Tags: race\n"))
        with self.subTest("new file"):
            new_file = "new file mode 100644\nindex 0..2\n"
            self.assertIsNone(edit("sql", "+-- Tags: no-msan\n", header=new_file))

    def test_build_flags_mirror_collect_build_flags(self):
        for cxx_flags, build_type, expected in (
            ("-fsanitize=address,undefined", "RelWithDebInfo", {"asan", "ubsan", "release"}),
            ("-fsanitize=memory", "RelWithDebInfo", {"msan", "release"}),
            ("-O0", "Debug", {"debug"}),
            ("-fsanitize=cfi-vcall,cfi-derived-cast", "RelWithDebInfo", {"release"}),
            ("", "", None),  # `clickhouse local` failed
        ):
            output = f"CXX_FLAGS\t-g {cxx_flags}\nBUILD_TYPE\t{build_type}" if build_type else ""
            probe = patch("ci.jobs.scripts.find_tests.Shell.get_output", return_value=output)
            with self.subTest(cxx_flags), probe:
                self.assertEqual(Targeting.get_build_flags("clickhouse"), expected)

    def test_changed_tests_leave_out_tag_edits_that_do_not_apply(self):
        import os
        import tempfile
        from pathlib import Path

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(tmp.name)
        root = Path("tests/queries/0_stateless")
        root.mkdir(parents=True)
        sections = {}
        for name, old, new in (
            ("00001_tag.sh", "# Tags: race", "# Tags: race, no-msan"),
            ("00002_ref.sql", "-- Tags: race", "-- Tags: race, no-msan"),
            ("00002_ref.reference", "1", "2"),
            ("00003_long.sql", "-- Tags: no-msan", "-- Tags: long"),
            ("00004_asan.sql", "-- Tags: no-tsan", "-- Tags: no-asan"),
        ):
            path = root / name
            path.write_text(f"{new}\n")
            header = f"diff --git a/{path} b/{path}\nindex 1..2 100644\n--- a/{path}\n+++ b/{path}"
            sections[name] = f"{header}\n@@ -1 +1 @@\n-{old}\n+{new}\n"
        info = SimpleNamespace(job_name="Stateless tests", is_local_run=False, pr_number=1)
        info.get_changed_files = lambda: [str(root / name) for name in sections]
        target = Targeting(info)
        sanitizer = "CXX_FLAGS\t-fsanitize={}\nBUILD_TYPE\tRelWithDebInfo"
        debug = "CXX_FLAGS\t-g\nBUILD_TYPE\tDebug"

        def select(build_options, binary="ci/tmp/clickhouse", diff="".join(sections.values())):
            probe = patch("ci.jobs.scripts.find_tests.Shell.get_output", return_value=build_options)
            with patch.object(target, "get_diff_text", return_value=diff), probe as get_output:
                return target.get_changed_tests(binary=binary), get_output

        every = ["00001_tag.", "00002_ref.", "00003_long.", "00004_asan."]
        tests, probe = select(sanitizer.format("address,undefined"), binary=None)
        self.assertEqual(tests, every)
        probe.assert_not_called()
        asan_ubsan = select(sanitizer.format("address,undefined"))[0]
        self.assertEqual(asan_ubsan, ["00002_ref.", "00003_long.", "00004_asan."])
        msan = select(sanitizer.format("memory"))[0]
        self.assertEqual(msan, ["00001_tag.", "00002_ref.", "00003_long."])
        self.assertEqual(select(debug)[0], ["00002_ref.", "00003_long."])
        self.assertEqual(select("")[0], every)  # the probe failed
        with patch.object(target, "get_diff_text", side_effect=RuntimeError("PR head changed")):
            self.assertEqual(target.get_changed_tests(binary="ci/tmp/clickhouse"), every)
        tests, probe = select(debug, diff=sections["00002_ref.reference"])
        self.assertEqual(tests, every)
        probe.assert_not_called()

    def test_merge_queue_diff_is_the_queued_prs(self):
        info = SimpleNamespace(
            job_name="Stateless tests",
            repo_name="ClickHouse/ClickHouse",
            is_local_run=False,
            pr_number=0,
            is_merge_queue_event=True,
            linked_pr_number=7,
            sha="merge-group",
        )
        metadata = SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"head": {"sha": "head"}, "base": {"sha": "base"}},
        )
        diff = SimpleNamespace(raise_for_status=lambda: None, text=FIXTURE_DIFF)
        with patch("requests.get", side_effect=[metadata, diff]) as get:
            self.assertEqual(Targeting(info).get_diff_text(), FIXTURE_DIFF)
        self.assertTrue(get.call_args_list[0].args[0].endswith("/pulls/7"))
        self.assertTrue(get.call_args_list[1].args[0].endswith("/compare/base...head"))

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
