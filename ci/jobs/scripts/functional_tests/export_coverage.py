import json
import shlex
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.coverage_selection import (
    build_candidate_query,
    canonical_coverage_path,
    parse_rows,
    rank_candidates,
    snapshot_predicate,
    sql_string,
)
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG
from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


class CoverageExporter:
    LOGS_SAVER_CLIENT_OPTIONS = "--max_memory_usage 10G --max_threads 1 --max_result_rows 0 --max_result_bytes 0 --max_rows_to_read 0 --max_rows_to_read_leaf 0 --max_bytes_to_read 0 --max_execution_time 0 --max_execution_time_leaf 0 --max_estimated_execution_time 0"

    def __init__(
        self,
        src: ClickHouseProc,
        dest: CIDBCluster,
        job_name: str,
        check_start_time="",
        to_file=False,
        events_path=None,
    ):
        self.src = src
        self.dest = dest
        assert to_file or self.dest.is_ready(), "Destination cluster is not ready"
        self.job_name = job_name
        self.check_start_time = check_start_time or Utils.timestamp_to_str(
            Utils.timestamp()
        )
        self.to_file = to_file
        self.events_path = events_path
        self.shard = self.job_name.rsplit(", ", 1)[-1].rstrip(")")
        self.metadata_path = (
            Path(temp_dir) / f"coverage-export-{self.shard.split('/')[0]}.json"
        )

    def _query(self, query):
        command = (
            f"cd {shlex.quote(self.src.run_path0)} && clickhouse local {self.LOGS_SAVER_CLIENT_OPTIONS} "
            "--only-system-tables --stacktrace --config-file=/etc/clickhouse-server/config.xml "
            f"--path {shlex.quote(self.src.run_path0)} --query {shlex.quote(query)} "
            "-- --zookeeper.implementation=testkeeper"
        )
        rc, stdout, stderr = Shell.get_res_stdout_stderr(command, verbose=False)
        if rc:
            # Remote insertion queries contain credentials; do not echo the command.
            raise RuntimeError(
                f"Coverage query failed (exit {rc}); inspect the local server log"
            )
        return stdout

    def do(self):
        # Disk definitions use the server configuration, even in the local process.
        Shell.check(
            f"sed -i 's|<log>.*</log>|<log>{self.src.CH_LOCAL_LOG}</log>|' /etc/clickhouse-server/config.xml"
        )
        Shell.check(
            f"sed -i 's|<errorlog>.*</errorlog>|<errorlog>{self.src.CH_LOCAL_ERR_LOG}</errorlog>|' /etc/clickhouse-server/config.xml"
        )
        info = Info()
        metadata = {
            "coverage_run_id": str(info.run_id),
            "commit_sha": info.sha,
            "shard": self.shard,
            "check_start_time": self.check_start_time,
            "check_name": self.job_name,
            "path_version": SELECTION_CONFIG.path_version,
            "randomized_settings": True,
            "status": "validating",
            "snapshot_identity": "legacy-shard-timestamp",
        }
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
        canonical_file = "replaceRegexpOne(file, '^([.]/)+', '')"
        # Generated protobuf files are build outputs, not repository source
        # coordinates. Keep their inventory visible but do not publish them as source.
        generated_prefix = "ci/tmp/build/"
        source = (
            "system.coverage_log FINAL WHERE notEmpty(test_name) AND test_name != '_selftest' "
            f"AND NOT startsWith({canonical_file}, {sql_string(generated_prefix)})"
        )
        stats = parse_rows(self._query(f"""
            SELECT test_name, count() AS rows, groupUniqArray(5)(file) AS sample_files
            FROM {source}
            GROUP BY test_name FORMAT JSONEachRow
        """))
        exported_tests = {
            row["test_name"] for row in stats if row["test_name"] != "_selftest"
        }
        if not exported_tests:
            raise RuntimeError(
                "Per-test coverage is empty; no useful shard was published"
            )
        paths = parse_rows(
            self._query(
                "SELECT DISTINCT file FROM system.coverage_log FINAL FORMAT JSONEachRow"
            )
        )
        normalized_paths = [canonical_coverage_path(row["file"]) for row in paths]
        metadata["excluded_generated_files"] = sorted(
            path for path in normalized_paths if path.startswith(generated_prefix)
        )
        events = []
        if self.events_path:
            events = [
                json.loads(line)
                for line in Path(self.events_path).read_text().splitlines()
            ]
            armed = {
                event["test"] for event in events if event["event"] == "coverage_armed"
            }
            if not armed:
                raise RuntimeError("No test armed per-test coverage")
            missing = sorted(armed - exported_tests)
            if missing:
                raise RuntimeError(
                    f"Executed tests have no exported coverage (including failed tests): {missing}"
                )
        metadata.update(
            {
                "exported_tests": sorted(exported_tests),
                "rows": sum(int(row["rows"]) for row in stats),
                "executed_tests": sorted(
                    {event["test"] for event in events if event["event"] == "started"}
                ),
                "settings_fingerprints": sorted(
                    {event["settings_fingerprint"] for event in events}
                ),
            }
        )
        # Validate before publication. Historical dotted paths are normalized at
        # the exporter boundary; absolute paths are rejected above.
        if self.to_file:
            directory = Path(temp_dir) / "system_tables"
            directory.mkdir(parents=True, exist_ok=True)
            self._query(
                f"SELECT time, test_name, {canonical_file} AS file, line_start, line_end, min_depth, branch_flag "
                f"FROM {source} INTO OUTFILE {sql_string(str(directory / 'coverage_log.tsv'))} FORMAT TSVWithNamesAndTypes"
            )
        else:
            remote = (
                f"remoteSecure({sql_string(self.dest.url.removeprefix('https://'))}, "
                f"'default.checks_coverage_lines', {sql_string(self.dest.user)}, {sql_string(self.dest.pwd)})"
            )
            # The legacy table already accepts second-resolution timestamps. Do
            # not round new exports to an hour or conflate different attempts.
            self._query(
                f"INSERT INTO FUNCTION {remote} "
                f"SELECT {canonical_file}, line_start, line_end, "
                f"toDateTime({sql_string(self.check_start_time)}, 'UTC'), {sql_string(self.job_name)}, "
                f"test_name, min_depth, branch_flag FROM {source}"
            )
            cidb = CIDB(self.dest.url, self.dest.user, self.dest.pwd)
            snapshot = [
                {"check_start_time": self.check_start_time, "check_name": self.job_name}
            ]
            remote_tests = parse_rows(
                cidb.query(
                    f"SELECT DISTINCT test_name FROM checks_coverage_lines WHERE {snapshot_predicate(snapshot)} FORMAT JSONEachRow",
                    log_level="",
                )
            )
            if exported_tests != {row["test_name"] for row in remote_tests}:
                raise RuntimeError(
                    "Post-export test inventory differs from the executed shard"
                )
            # Exercise the production query and scorer against this exact export.
            seeds = parse_rows(self._query(f"""
                SELECT {canonical_file} AS file, line_start, line_end
                FROM {source}
                GROUP BY file, line_start, line_end
                HAVING line_end >= line_start
                   AND line_end - line_start + 1 <= {SELECTION_CONFIG.narrow_region_max_lines}
                   AND uniqExact(test_name) <= {SELECTION_CONFIG.max_precise_region_owners}
                ORDER BY line_end - line_start, file, line_start LIMIT 1 FORMAT JSONEachRow
            """))
            if not seeds:
                raise RuntimeError(
                    "Export has no precise coverage region for the selector smoke"
                )
            changed = [
                (canonical_coverage_path(seeds[0]["file"]), int(seeds[0]["line_start"]))
            ]
            query = build_candidate_query(changed, {}, snapshot)
            regions = parse_rows(cidb.query(query, log_level=""))
            if not rank_candidates(regions, changed, {}, snapshot):
                raise RuntimeError(f"Post-export selector smoke failed: {query}")
            metadata["selector_smoke"] = {
                "status": "OK",
                "query": query,
                "region_count": len(regions),
            }
            indirect_rows = int(
                self._query(
                    "SELECT count() FROM system.coverage_indirect_calls FINAL WHERE notEmpty(test_name)"
                ).strip()
            )
            if indirect_rows:
                remote_indirect = remote.replace(
                    "default.checks_coverage_lines",
                    "default.checks_coverage_indirect_calls",
                )
                self._query(
                    f"INSERT INTO FUNCTION {remote_indirect} "
                    f"SELECT toDateTime({sql_string(self.check_start_time)}, 'UTC'), {sql_string(self.job_name)}, "
                    "test_name, caller_name_hash, caller_func_hash, callee_offset, call_count "
                    "FROM system.coverage_indirect_calls FINAL WHERE notEmpty(test_name)"
                )
        metadata["status"] = "OK"
        self.metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
        print(f"Exported and validated coverage for {len(exported_tests)} tests")
