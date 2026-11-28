"""LLVM coverage for a serial performance-test run on a dedicated server."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path


def query_hash(query):
    return hashlib.sha256(
        json.dumps(query, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


def tsv_escape(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


class CoverageCollector:
    def __init__(self, client, directory, test_name, queries, warm_runs):
        self.client = client
        self.directory = Path(directory)
        self.test_name = test_name
        # The runner can mark disabled queries explicitly. Keep active query
        # hashes compatible with older manifests without a false flag.
        self.queries = [
            {key: value for key, value in query.items() if key != "disabled" or value}
            for query in queries
        ]
        self.warm_runs = warm_runs
        self.names = []
        self.directory.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.directory / f"{test_name}.json"
        self.lines_path = self.directory / f"{test_name}.lines.tsv"
        if self.manifest_path.exists() or self.lines_path.exists():
            raise RuntimeError(f"Coverage output already exists for {test_name}")
        options = dict(
            client.execute(
                "SELECT name, value FROM system.build_options "
                "WHERE name IN ('WITH_COVERAGE', 'WITH_COVERAGE_DEPTH')"
            )
        )
        if any(
            options.get(name) not in ("ON", "1")
            for name in ("WITH_COVERAGE", "WITH_COVERAGE_DEPTH")
        ):
            raise RuntimeError(
                "Coverage collection requires WITH_COVERAGE=ON and WITH_COVERAGE_DEPTH=ON"
            )
        # Same sink and key as `clickhouse-test`. The caller owns this server:
        # coverage attribution and LLVM counters are process-global.
        client.execute(
            """CREATE TABLE IF NOT EXISTS system.coverage_log
            (time DateTime, test_name LowCardinality(String), file LowCardinality(String),
             line_start UInt32, line_end UInt32, min_depth UInt8, branch_flag UInt8)
            ENGINE = ReplacingMergeTree(time) ORDER BY (test_name, file, line_start, line_end)"""
        )
        client.execute(
            """CREATE TABLE IF NOT EXISTS system.coverage_indirect_calls
            (test_name LowCardinality(String), callee_offset UInt64, caller_name_hash UInt64,
             caller_func_hash UInt64, call_count UInt64)
            ENGINE = ReplacingMergeTree(call_count) ORDER BY (test_name, callee_offset)"""
        )
        self.stop()
        # Nothing from an earlier/failed test is part of this test's snapshot.
        client.execute("TRUNCATE TABLE system.coverage_log")
        client.execute("TRUNCATE TABLE system.coverage_indirect_calls")

    def start(self, phase, query_index=None):
        name = self.test_name
        if query_index is not None:
            name += f".query{query_index}.{query_hash(self.queries[query_index])[:16]}"
        name += f".{phase}"
        self.client.execute("SYSTEM SET COVERAGE TEST %(name)s", {"name": name})
        self.names.append(
            {"test_name": name, "query_index": query_index, "phase": phase}
        )

    def stop(self):
        self.client.execute("SYSTEM SET COVERAGE TEST ''")

    @contextmanager
    def phase(self, phase, query_index):
        self.start(phase, query_index)
        try:
            yield
        finally:
            self.stop()

    def collect_query(self, query_index, execute):
        if self.queries[query_index].get("disabled"):
            raise ValueError("Cannot collect a disabled query")
        # No timing loop or profiler pass. The first execution also warms the
        # query; the repeated executions retain their union under one identity.
        with self.phase("first", query_index):
            execute(0)
        with self.phase("warm", query_index):
            for run in range(1, self.warm_runs + 1):
                execute(run)

    def finish(self, settings):
        self.stop()
        expected = {f"{self.test_name}.setup", f"{self.test_name}.teardown"}
        for index, query in enumerate(self.queries):
            if not query.get("disabled"):
                expected.update(
                    f"{self.test_name}.query{index}.{query_hash(query)[:16]}.{phase}"
                    for phase in ("first", "warm")
                )
        if (
            len(self.names) != len(expected)
            or {entry["test_name"] for entry in self.names} != expected
        ):
            raise RuntimeError("Incomplete coverage query/setup/teardown phases")
        seen = set()
        rows = 0
        # Write in the same six-column format as the integration file sink.
        # Only successful tests call `finish`; the manifest is the completion marker.
        with self.lines_path.open("x", encoding="utf-8") as out:
            for row in self.client.execute_iter(
                "SELECT test_name, file, line_start, line_end, min_depth, branch_flag "
                "FROM system.coverage_log FINAL WHERE notEmpty(test_name)"
            ):
                seen.add(row[0])
                out.write("\t".join(map(tsv_escape, row)) + "\n")
                rows += 1
        missing = {entry["test_name"] for entry in self.names} - seen
        if missing or not rows:
            raise RuntimeError(
                f"Empty coverage phases: {sorted(missing)} ({rows} total rows)"
            )
        manifest = {
            "version": 1,
            "test": self.test_name,
            "warm_runs": self.warm_runs,
            "settings": settings,
            "queries": [
                {"index": index, "sha256": query_hash(query), **query}
                for index, query in enumerate(self.queries)
            ],
            "phases": self.names,
            "rows": rows,
        }
        self.manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
