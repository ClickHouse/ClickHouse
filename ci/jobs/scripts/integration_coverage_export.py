import shlex
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.coverage_selection import sql_string
from ci.praktika.utils import Shell, Utils

LINES_STRUCTURE = "test_name String, file String, line_start UInt32, line_end UInt32, min_depth UInt8, branch_flag UInt8"
INDIRECT_CALLS_STRUCTURE = "test_name String, caller_name_hash UInt64, caller_func_hash UInt64, callee_offset UInt64, call_count UInt64"


class IntegrationCoverageExporter:
    """Export the per-module coverage dumped by `tests/integration/helpers/cluster.py`.

    Every instance of a module writes its own dump, one row per region per flush, so the
    rows are merged per module (over instances and server restarts) before they are
    inserted into the CIDB tables that hold the stateless per-test coverage. `test_name`
    is the module path relative to `tests/integration`, e.g. `test_storage_s3/test.py`.

    Without XRay (the per-test build does not enable it) `min_depth` is the entry count
    of the function, saturated at 254 (see `getCurrentCoveredNameRefs`), so the module's
    value is the saturated sum over flushes, as is `call_count` of the indirect calls.
    """

    def __init__(self, clickhouse_path: str, coverage_dir: str, dest: CIDBCluster, job_name: str):
        self.clickhouse_path = clickhouse_path
        self.coverage_dir = Path(coverage_dir)
        self.dest = dest
        self.job_name = job_name
        self.check_start_time = Utils.timestamp_to_str(Utils.timestamp())

    def _query(self, query):
        # The binary is the instrumented one; keep its own profile out of the way.
        env = f"LLVM_PROFILE_FILE={shlex.quote(str(self.coverage_dir / 'export-%m.profraw'))}"
        rc, stdout, _ = Shell.get_res_stdout_stderr(
            f"{env} {shlex.quote(self.clickhouse_path)} local --query {shlex.quote(query)}",
            verbose=False,
        )
        if rc:
            # Remote insertion queries contain credentials; do not echo the command.
            raise RuntimeError(f"Coverage export query failed (exit {rc})")
        return stdout

    def _source(self, suffix, structure):
        files = sorted(self.coverage_dir.glob(f"*.{suffix}.tsv"))
        if not any(f.stat().st_size for f in files):
            return None
        return f"file({sql_string(str(self.coverage_dir / f'*.{suffix}.tsv'))}, 'TSV', {sql_string(structure)})"

    def _remote(self, table):
        return (
            f"remoteSecure({sql_string(self.dest.url.removeprefix('https://'))}, "
            f"{sql_string(table)}, {sql_string(self.dest.user)}, {sql_string(self.dest.pwd)})"
        )

    def do(self):
        lines = self._source("lines", LINES_STRUCTURE)
        if not lines:
            raise RuntimeError(f"Per-module coverage is empty: no rows in {self.coverage_dir}")
        stats = self._query(f"SELECT count(), uniqExact(test_name) FROM {lines}").split()
        print(f"Coverage dumps: {stats[0]} rows, {stats[1]} modules")
        assert self.dest.is_ready(), "Destination cluster is not ready"
        check_start_time = f"toDateTime({sql_string(self.check_start_time)}, 'UTC')"
        check_name = sql_string(self.job_name)
        # Same key as `system.coverage_log`. `branch_flag` is a property of the region and
        # agrees between flushes unless several regions share one span; then take the one
        # of the flush with the most entries.
        self._query(
            f"INSERT INTO FUNCTION {self._remote('default.checks_coverage_lines')} "
            f"SELECT file, line_start, line_end, {check_start_time}, {check_name}, test_name, "
            "least(sum(min_depth), 254), argMax(branch_flag, min_depth) "
            f"FROM {lines} GROUP BY test_name, file, line_start, line_end"
        )
        indirect_calls = self._source("indirect_calls", INDIRECT_CALLS_STRUCTURE)
        if indirect_calls:
            self._query(
                f"INSERT INTO FUNCTION {self._remote('default.checks_coverage_indirect_calls')} "
                f"SELECT {check_start_time}, {check_name}, test_name, "
                "argMax(caller_name_hash, call_count), argMax(caller_func_hash, call_count), "
                "callee_offset, sum(call_count) "
                f"FROM {indirect_calls} GROUP BY test_name, callee_offset"
            )
        print("Coverage export completed")
