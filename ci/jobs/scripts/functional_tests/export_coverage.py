import shlex
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.coverage_selection import (
    build_candidate_query,
    build_selector_smoke_seed_query,
    parse_rows,
    rank_candidates,
    sql_string,
)
from ci.praktika.cidb import CIDB
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
    ):
        self.src = src
        self.dest = dest
        assert to_file or self.dest.is_ready(), "Destination cluster is not ready"
        self.job_name = job_name
        self.check_start_time = check_start_time or Utils.timestamp_to_str(
            Utils.timestamp()
        )
        self.to_file = to_file

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
        source = "system.coverage_log FINAL WHERE notEmpty(test_name) AND test_name != '_selftest'"
        rows = int(self._query(f"SELECT count() FROM {source}").strip())
        if not rows:
            raise RuntimeError("Per-test coverage is empty")
        print(f"Coverage log: {rows} rows")
        # Preserve recorded paths; the selector interprets source coordinates.
        if self.to_file:
            directory = Path(temp_dir) / "system_tables"
            directory.mkdir(parents=True, exist_ok=True)
            self._query(
                "SELECT time, test_name, file, line_start, line_end, min_depth, branch_flag "
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
                "SELECT file, line_start, line_end, "
                f"toDateTime({sql_string(self.check_start_time)}, 'UTC'), {sql_string(self.job_name)}, "
                f"test_name, min_depth, branch_flag FROM {source}"
            )
            cidb = CIDB(self.dest.url, self.dest.user, self.dest.pwd)
            snapshot = [
                {"check_start_time": self.check_start_time, "check_name": self.job_name}
            ]
            # Exercise the production query and scorer against this exact export.
            seeds = parse_rows(self._query(build_selector_smoke_seed_query(source)))
            if not seeds:
                raise RuntimeError(
                    "Export has no precise coverage region for the selector smoke"
                )
            changed = [(seeds[0]["file"], int(seeds[0]["line_start"]))]
            query = build_candidate_query(changed, {}, snapshot)
            regions = parse_rows(cidb.query(query, log_level=""))
            if not rank_candidates(regions, changed, {}, snapshot):
                raise RuntimeError(f"Post-export selector smoke failed: {query}")
            print(f"Post-export selector smoke passed: {len(regions)} precise regions")
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
        print("Coverage export completed")
