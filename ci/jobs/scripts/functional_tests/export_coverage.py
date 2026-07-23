from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


class CoverageExporter:
    LOGS_SAVER_CLIENT_OPTIONS = "--max_memory_usage 10G --max_threads 1 --max_result_rows 0 --max_result_bytes 0 --max_bytes_to_read 0 --max_execution_time 0 --max_execution_time_leaf 0 --max_estimated_execution_time 0"

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

    def do(self):
        command_args = self.LOGS_SAVER_CLIENT_OPTIONS
        # command_args += f" --config-file={self.ch_config_dir}/config.xml"
        command_args += " --only-system-tables --stacktrace"
        # we need disk definitions for S3 configurations, but it is OK to always use server config

        command_args += " --config-file=/etc/clickhouse-server/config.xml"
        # Change log files for local in config.xml as command args do not override
        Shell.check(
            f"sed -i 's|<log>.*</log>|<log>{self.src.CH_LOCAL_LOG}</log>|' /etc/clickhouse-server/config.xml"
        )
        Shell.check(
            f"sed -i 's|<errorlog>.*</errorlog>|<errorlog>{self.src.CH_LOCAL_ERR_LOG}</errorlog>|' /etc/clickhouse-server/config.xml"
        )
        # FIXME: Hack for s3_with_keeper (note, that we don't need the disk,
        # the problem is that whenever we need disks all disks will be
        # initialized [1])
        #
        #   [1]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        #   [2]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        command_args_post = f"-- --zookeeper.implementation=testkeeper"

        Shell.check(
            f"rm -rf {temp_dir}/system_tables && mkdir -p {temp_dir}/system_tables"
        )
        table = "coverage_log"
        path_arg = f" --path {self.src.run_path0}"

        stats_query = (
            f"SELECT count() AS rows, countIf(notEmpty(test_name)) AS rows_with_test, "
            f"uniqExact(test_name) AS tests, uniqExact(file) AS files, "
            f"round(avg(toUInt32(min_depth))) AS avg_min_depth "
            f"FROM system.{table} FINAL"
        )
        stats_cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{stats_query}" {command_args_post}'
        rc_stats, stdout_stats, stderr_stats = Shell.get_res_stdout_stderr(stats_cmd, verbose=True)
        if rc_stats != 0:
            raise RuntimeError(
                f"Failed to read system.{table} statistics, stderr: {stderr_stats}"
            )
        else:
            print(f"Coverage log statistics: {stdout_stats}")
            rows = int(stdout_stats.strip().split("\t")[0])
            if rows == 0:
                raise RuntimeError(
                    f"system.{table} is empty — per-test coverage collection is broken. "
                    "Check that the server was built with WITH_COVERAGE=ON and that "
                    "SYSTEM SET COVERAGE TEST flushed data correctly."
                )

        if not self.to_file:
            query = (
                f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_lines', '{self.dest.user}', '{self.dest.pwd}') "
                "SELECT file, line_start, line_end, "
                f"toStartOfHour(toDateTime('{self.check_start_time}')) AS check_start_time, "
                f"'{self.job_name}' AS check_name, "
                "test_name, min_depth, branch_flag "
                f"FROM system.{table} FINAL "
                "WHERE notEmpty(test_name) AND notEmpty(file)"
            )
            cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
            rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
            if stdout:
                print(f"Export stdout: {stdout}")
            if stderr:
                print(f"Export stderr: {stderr}")
            if rc != 0:
                raise RuntimeError(f"Failed to export coverage table: {table}")
        else:
            query = (
                "SELECT time, test_name, file, line_start, line_end, min_depth, branch_flag "
                f"FROM system.{table} FINAL "
                "WHERE notEmpty(test_name) AND notEmpty(file) "
                f"INTO OUTFILE '{temp_dir}/system_tables/{table}.tsv' "
                "FORMAT TSVWithNamesAndTypes"
            )
            cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}'
            rc, stdout, stderr = Shell.get_res_stdout_stderr(cmd, verbose=True)
            if rc != 0:
                raise RuntimeError(f"Failed to export coverage table to file: {table}")

        # Export indirect call observations if any were collected.
        # Note: uses ReplacingMergeTree with ORDER BY (test_name, callee_offset) so FINAL
        # correctly deduplicates; plain MergeTree would reject FINAL with error 181.
        if not self.to_file:
            ic_stats_query = (
                "SELECT count() AS rows, uniqExact(test_name) AS tests, "
                "uniqExact(callee_offset) AS unique_callees "
                "FROM system.coverage_indirect_calls FINAL WHERE notEmpty(test_name)"
            )
            ic_stats_cmd = (
                f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} '
                f'--query "{ic_stats_query}" {command_args_post}'
            )
            rc_s, stdout_s, _ = Shell.get_res_stdout_stderr(ic_stats_cmd, verbose=False)
            if rc_s == 0 and stdout_s.strip():
                print(f"Indirect calls statistics: {stdout_s.strip()}")
                ic_count = int(stdout_s.strip().split("\t")[0])
            else:
                ic_count = 0

            if ic_count > 0:
                ic_query = (
                    f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_indirect_calls', '{self.dest.user}', '{self.dest.pwd}') "
                    f"SELECT toStartOfHour(toDateTime('{self.check_start_time}')) AS check_start_time, "
                    f"'{self.job_name}' AS check_name, "
                    "test_name, caller_name_hash, caller_func_hash, callee_offset, call_count "
                    "FROM system.coverage_indirect_calls FINAL "
                    "WHERE notEmpty(test_name)"
                )
                ic_cmd = f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{ic_query}" {command_args_post}'
                rc_ic, _, stderr_ic = Shell.get_res_stdout_stderr(ic_cmd, verbose=True)
                if rc_ic != 0:
                    raise RuntimeError(
                        f"Failed to export indirect calls to CIDB: {stderr_ic[:400]}"
                    )
                print(f"Exported {ic_count} indirect call rows to CIDB")
            else:
                print("No indirect call data to export (value profiling inactive or no calls recorded)")
