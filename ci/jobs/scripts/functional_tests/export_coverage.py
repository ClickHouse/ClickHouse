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
        res = True

        for table in ["coverage_log"]:
            path_arg = f" --path {self.src.run_path0}"

            if not self.to_file:
                query = (
                    f"INSERT INTO FUNCTION remoteSecure('{self.dest.url.removeprefix('https://')}', 'default.checks_coverage_inverted', '{self.dest.user}', '{self.dest.pwd}') "
                    "SELECT DISTINCT "
                    "arrayJoin(symbol) AS symbol, "
                    f"'{self.check_start_time}' AS check_start_time, "
                    f"'{self.job_name}' AS check_name, "
                    "test_name "
                    f"FROM system.{table}"
                )
                res = Shell.check(
                    f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}',
                    verbose=False,  # IMPORTANT: do not change
                )
            else:
                query = (
                    "SELECT "
                    "time, "
                    "arrayJoin(symbol) AS symbol, "
                    "test_name "
                    f"FROM system.{table} "
                    f"INTO OUTFILE '{temp_dir}/system_tables/{table}.tsv' "
                    "FORMAT TSVWithNamesAndTypes"
                )
                res = Shell.check(
                    f'cd {self.src.run_path0} && clickhouse local {command_args} {path_arg} --query "{query}" {command_args_post}',
                    verbose=True,
                )

            if not res:
                print(f"ERROR: Failed to export coverage table: {table}")
        return res
