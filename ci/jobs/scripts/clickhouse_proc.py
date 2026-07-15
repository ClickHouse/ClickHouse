import glob
import json as json_module
import os
import platform
import signal
import subprocess
import sys
import time
import threading
import traceback
import uuid
from collections import defaultdict
from pathlib import Path
from typing import List

from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.jobs.scripts.server_cleanup import kill_leftover_server_processes
from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_dir = f"{repo_dir}/ci/tmp"
p_temp_dir = Path(temp_dir)

LOG_EXPORT_CONFIG_TEMPLATE = """
remote_servers:
    {CLICKHOUSE_CI_LOGS_CLUSTER}:
        shard:
            replica:
                secure: 1
                user: '{CLICKHOUSE_CI_LOGS_USER}'
                host: '{CLICKHOUSE_CI_LOGS_HOST}'
                port: 9440
                password: '{CLICKHOUSE_CI_LOGS_PASSWORD}'
"""
CLICKHOUSE_CI_LOGS_CLUSTER = "system_logs_export"
CLICKHOUSE_CI_LOGS_USER = "ci"


class ClickHouseProc:
    MINIO_LOG = f"{temp_dir}/minio.log"
    AZURITE_LOG = f"{temp_dir}/azurite.log"
    KAFKA_LOG = f"{temp_dir}/kafka.log"
    LOGS_SAVER_CLIENT_OPTIONS = "--max_memory_usage 10G --max_threads 1 --max_rows_to_read=0 --max_result_rows 0 --max_result_bytes 0 --max_bytes_to_read 0 --max_execution_time 0 --max_execution_time_leaf 0 --max_estimated_execution_time 0"
    DMESG_LOG = f"{temp_dir}/dmesg.log"
    # TODO: run servers in  dedicated wds to keep trash localised
    WD0 = f"{temp_dir}/ft_wd0"
    WD1 = f"{temp_dir}/ft_wd1"
    WD2 = f"{temp_dir}/ft_wd2"
    CH_LOCAL_LOG = f"{temp_dir}/clickhouse-local.log"
    CH_LOCAL_ERR_LOG = f"{temp_dir}/clickhouse-local.err.log"
    # Per-table wall-clock cap for dump_system_tables (seconds). One stuck dump
    # must not exhaust the job's 9000s budget and get the whole job SIGKILLed.
    DUMP_SYSTEM_TABLE_TIMEOUT = 600

    def __init__(
        self,
        is_db_replicated=False,
        is_shared_catalog=False,
        is_per_test_coverage=False,
        ch_config_dir="/etc/clickhouse-server",
        ch_var_lib_dir="/var/lib/clickhouse",
    ):
        self.is_db_replicated = is_db_replicated
        self.is_shared_catalog = is_shared_catalog
        self.is_per_test_coverage = is_per_test_coverage
        self.ch_config_dir = ch_config_dir
        self.ch_var_lib_dir = ch_var_lib_dir
        self.run_path0 = f"{temp_dir}/run_r0"
        self.run_path1 = f"{temp_dir}/run_r1"
        self.run_path2 = f"{temp_dir}/run_r2"
        self.log_dir = f"{temp_dir}/var/log/clickhouse-server"
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.config_file = f"{self.ch_config_dir}/config.xml"
        # NOTE: should be the same for all replicas (for database replicated), since some tests uses CREATE TABLE Engine=File(${USER_FILES_PATH})
        self.user_files_path = f"{self.run_path0}/user_files"
        self.test_output_file = f"{temp_dir}/test_result.txt"
        self.command = f"clickhouse-server --config-file {self.config_file} --pid-file {self.pid_file} -- --path {self.run_path0} --user_files_path {self.user_files_path} --top_level_domains_path {self.ch_config_dir}/top_level_domains --logger.stderr {self.log_dir}/stderr.log"
        self.ch_config_dir_replica_1 = "/etc/clickhouse-server1"
        self.config_file_replica_1 = f"{self.ch_config_dir_replica_1}/config.xml"
        self.ch_config_dir_replica_2 = "/etc/clickhouse-server2"
        self.config_file_replica_2 = f"{self.ch_config_dir_replica_2}/config.xml"
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.pid_file_replica_1 = (
            f"{self.ch_config_dir_replica_1}/clickhouse-server.pid"
        )
        self.pid_file_replica_2 = (
            f"{self.ch_config_dir_replica_2}/clickhouse-server.pid"
        )
        self.pid_0 = 0
        self.pid_1 = 0
        self.pid_2 = 0
        self.port = 9000
        self.port_1 = 19000
        self.port_2 = 29000
        self.replica_command_1 = f"clickhouse-server --config-file {self.config_file_replica_1} --pid-file {self.pid_file_replica_1} -- --path {self.run_path1} --user_files_path {self.user_files_path} --logger.stderr {self.log_dir}/stderr1.log --logger.log {self.log_dir}/clickhouse-server1.log --logger.errorlog {self.log_dir}/clickhouse-server1.err.log --tcp_port {self.port_1} --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 --mysql_port 19004 --postgresql_port 19005 --keeper_server.tcp_port 19181 --keeper_server.server_id 2 --prometheus.port 19988 --macros.replica r2"
        self.replica_command_2 = f"clickhouse-server --config-file {self.config_file_replica_2} --pid-file {self.pid_file_replica_2} -- --path {self.run_path2} --user_files_path {self.user_files_path} --logger.stderr {self.log_dir}/stderr2.log --logger.log {self.log_dir}/clickhouse-server2.log --logger.errorlog {self.log_dir}/clickhouse-server2.err.log --tcp_port {self.port_2} --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 --mysql_port 29004 --postgresql_port 29005 --keeper_server.tcp_port 29181 --keeper_server.server_id 3 --prometheus.port 29988 --macros.shard s2"
        self.proc = None
        self.proc_1 = None
        self.proc_2 = None
        self.pid = 0
        int(Utils.cpu_count() / 2)
        self.minio_proc = None
        self.azurite_proc = None
        self.kafka_proc = None
        # Concrete reason set by create_minio_log_tables() on failure, so the
        # caller can persist the real detail (e.g. the clickminio restart status)
        # into the step Result.info / CIDB instead of a generic note.
        self.minio_setup_error = None
        # Same idea for prepare_stateful_data(): the failing sub-command + its
        # ClickHouse error tail, so the re-prepare ERROR row carries the real
        # reason instead of the generic "failed to re-prepare stateful data".
        self.stateful_setup_error = None
        self.debug_artifacts = []
        self.extra_tests_results = []
        self.logs = []
        self.log_export_host, self.log_export_password = None, None
        self.system_db_uuid = None

        Utils.set_env("CLICKHOUSE_CONFIG_DIR", self.ch_config_dir)
        Utils.set_env("CLICKHOUSE_CONFIG", self.config_file)
        Utils.set_env(
            "CLICKHOUSE_SCHEMA_FILES", f"{self.ch_var_lib_dir}/format_schemas"
        )
        Utils.set_env("CLICKHOUSE_USER_FILES", f"{self.user_files_path}")
        Utils.clean_dir(Path(self.log_dir))

    # there should be one install and one start method instead of many for each job
    # job specifics should be a part of the job
    def install_configs(self):
        Path(f"{self.ch_config_dir}/config.d").mkdir(parents=True, exist_ok=True)
        with open(f"{self.ch_config_dir}/config.d/storage_conf_backups.xml", "w") as file:
            file.write(f"""
<clickhouse>
    <storage_configuration>
        <disks>
            <backups>
                <type>local</type>
                <path>{self.ch_var_lib_dir}/disks/backups/</path>
            </backups>
        </disks>
    </storage_configuration>
</clickhouse>
""")
        with open(f"{self.ch_config_dir}/config.d/filesystem_caches_path.xml", "w") as file:
            file.write(f"""
<clickhouse>
    <filesystem_caches_path>{self.ch_var_lib_dir}/filesystem_caches/</filesystem_caches_path>
    <custom_cached_disks_base_directory replace="replace">{self.ch_var_lib_dir}/filesystem_caches/</custom_cached_disks_base_directory>
</clickhouse>
""")

    def start_minio(self, test_type):
        os.environ["TEMP_DIR"] = f"{Utils.cwd()}/ci/tmp"
        command = [
            "./ci/jobs/scripts/functional_tests/setup_minio.sh",
            test_type,
            "./tests",
        ]
        with open(self.MINIO_LOG, "w") as log_file:
            self.minio_proc = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT
            )
        print(f"Started setup_minio.sh asynchronously with PID {self.minio_proc.pid}")

        # Wait for setup_minio.sh to fully exit, not just for the bucket to be
        # listable: the server's S3 disks authenticate at startup and need the
        # whole user/policy/ACL setup in place. The minio server is nohup'd and
        # outlives the script, so waiting on the script is safe. Its internal
        # waits are bounded (wait_for_it caps at 60s), so pad the timeout.
        try:
            returncode = self.minio_proc.wait(timeout=120)
        except subprocess.TimeoutExpired:
            print("Failed to start minio: setup_minio.sh did not finish in time")
            self.minio_proc.kill()
            return False
        if returncode != 0:
            print(f"setup_minio.sh exited with code {returncode}")
            return False

        # wait_for_it can exit 0 even if minio is down, so confirm the bucket.
        if not Shell.check("/mc ls clickminio/test", verbose=False, retries=3):
            print("Failed to start minio: bucket clickminio/test not reachable")
            return False
        return True

    def start_azurite(self):
        # Raise the open files limit before launching azurite-rs.
        # Each concurrent test query opens a TCP connection plus an in-memory
        # blob handle, and the default soft limit (1024) was exhausted under
        # parallel load, causing `accept error: Too many open files`.
        # Fall back to the hard limit if 1048576 cannot be set.
        command = (
            f"cd {temp_dir} && "
            "(ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)) && "
            "azurite-rs --host 0.0.0.0 --blob-port 10000 --silent --in-memory"
        )
        with open(self.AZURITE_LOG, "w") as log_file:
            self.azurite_proc = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT, shell=True
            )
        print(f"Started azurite-rs asynchronously with PID {self.azurite_proc.pid}")

        if Shell.check(
            "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:10000/ | grep -qE '400|200'",
            verbose=False,
            retries=6,
        ):
            return True
        print("Failed to start azurite-rs")
        return False

    def start_kafka(self):
        command = [
            "./ci/jobs/scripts/functional_tests/setup_kafka.sh",
        ]
        with open(self.KAFKA_LOG, "w") as log_file:
            self.kafka_proc = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT
            )
        print(f"Started setup_kafka.sh asynchronously with PID {self.kafka_proc.pid}")

        # setup_kafka.sh exits 0 only after broker AND schema registry are ready,
        # so wait on the script itself. Its own timeout is 60s; pad here.
        try:
            returncode = self.kafka_proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            print("Failed to start Kafka: setup_kafka.sh did not finish in time")
            return False
        if returncode != 0:
            print(f"setup_kafka.sh exited with code {returncode}")
            return False
        return True

    @staticmethod
    def log_cluster_config():
        return Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --config-logs-export-cluster ./tmp_ci/etc/clickhouse-server/config.d/system_logs_export.yaml",
            verbose=True,
        )

    @staticmethod
    def enable_thread_fuzzer_config():
        # For flaky check we also enable thread fuzzer
        os.environ["THREAD_FUZZER_CPU_TIME_PERIOD_US"] = "1000"
        os.environ["THREAD_FUZZER_SLEEP_PROBABILITY"] = "0.1"
        os.environ["THREAD_FUZZER_SLEEP_TIME_US_MAX"] = "100000"

        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_MIGRATE_PROBABILITY"] = "1"
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_MIGRATE_PROBABILITY"] = "1"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_MIGRATE_PROBABILITY"] = "1"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_MIGRATE_PROBABILITY"] = "1"

        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_PROBABILITY"] = "0.001"
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_PROBABILITY"] = "0.001"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_PROBABILITY"] = "0.001"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_PROBABILITY"] = "0.001"

        os.environ["THREAD_FUZZER_pthread_mutex_lock_BEFORE_SLEEP_TIME_US_MAX"] = "10000"
        os.environ["THREAD_FUZZER_pthread_mutex_lock_AFTER_SLEEP_TIME_US_MAX"] = "10000"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_BEFORE_SLEEP_TIME_US_MAX"] = "10000"
        os.environ["THREAD_FUZZER_pthread_mutex_unlock_AFTER_SLEEP_TIME_US_MAX"] = "10000"

    def set_memory_ratio(self, ratio):
        config = f"""<clickhouse>
    <max_server_memory_usage_to_ram_ratio>{ratio}</max_server_memory_usage_to_ram_ratio>
</clickhouse>
"""
        # In DBReplicated mode `install.sh` has already cloned
        # /etc/clickhouse-server into the two replica config dirs and `start`
        # launches all three servers, so the override must be written into every
        # replica config dir too - otherwise the extra replicas keep the default
        # 0.9 ratio and can still drive the host into a global OOM under the
        # heavier multi-server footprint (the very failure this cap prevents).
        config_dirs = [self.ch_config_dir]
        if self.is_db_replicated:
            config_dirs += [
                self.ch_config_dir_replica_1,
                self.ch_config_dir_replica_2,
            ]
        for config_dir in config_dirs:
            file_path = (
                f"{config_dir}/config.d/max_server_memory_usage_to_ram_ratio.xml"
            )
            with open(file_path, "w") as f:
                f.write(config)
            print(
                f"Set max_server_memory_usage_to_ram_ratio to {ratio} in {file_path}"
            )

    def _install_light(self):
        """
        Installs ClickHouse config into ci temporary directory, this way of installation does not require mounting /etc|var/clickhouse-server into docker container.
        To be used only with start_light(). This method is suitable for jobs that do not require complex configuration, such as clickbench.
        Jobs like functional tests are hard/not-reasonable to adapt to use this way of installation, thus they have to mount config and other directories into default directories.
        """
        Utils.add_to_PATH(temp_dir)
        commands = [
            f"mkdir -p {temp_dir}/users.d",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {temp_dir}",
            # make it ipv4 only
            f'sed -i "s|<!-- <listen_host>0.0.0.0</listen_host> -->|<listen_host>0.0.0.0</listen_host>|" {temp_dir}/config.xml',
            f"cp -r --dereference ./programs/server/config.d {temp_dir}",
            f"chmod +x {temp_dir}/clickhouse",
            f"ln -sf {temp_dir}/clickhouse {temp_dir}/clickhouse-server",
            f"ln -sf {temp_dir}/clickhouse {temp_dir}/clickhouse-client",
        ]
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        if not res:
            print("Failed to install ClickHouse config")

        return res

    def start_light(self):
        """
        Start ClickHouse server with config installed with _install_config()
        """
        print("Starting ClickHouse server")
        # check binary available and do decompression in the meantime
        assert Shell.check("clickhouse --version", verbose=True)
        kill_leftover_server_processes()
        self.pid_file = f"{temp_dir}/clickhouse-server.pid"
        self.start_cmd = f"{temp_dir}/clickhouse-server --config-file={temp_dir}/config.xml --pid-file {self.pid_file}"
        print("Command: ", self.start_cmd)
        self.log_fd = open(f"{self.log_dir}/clickhouse-server.log", "w")
        self.proc = subprocess.Popen(
            self.start_cmd, stderr=subprocess.STDOUT, stdout=self.log_fd, shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print("ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print("ClickHouse server ready")
        else:
            print("ClickHouse server NOT ready")

        # wait_ready() flushes system logs on its success path (pre-creating the
        # system log tables once the server is listening).
        self.save_system_metadata_files_from_remote_database_disk()
        return res

    def install_clickbench_config(self):
        res = self._install_light()
        if not res:
            return False

        # tweak for clickbench
        content = """
profiles:
    default:
        allow_introspection_functions: 1
"""
        file_path = f"{temp_dir}/users.d/allow_introspection_functions.yaml"
        with open(file_path, "w") as file:
            file.write(content)
        return True

    def install_fuzzer_config(self):
        res = self._install_light()
        if not res:
            return False
        commands = [
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml {temp_dir}/users.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/limit-recursion-settings.xml {temp_dir}/users.d",
        ]

        c1 = """
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.75</max_server_memory_usage_to_ram_ratio>
</clickhouse>
"""
        file_path = f"{temp_dir}/config.d/max_server_memory_usage_to_ram_ratio.xml"
        with open(file_path, "w") as file:
            file.write(c1)

        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def install_vector_search_config(self):
        # Large values are set, ClickHouse will auto downsize
        c1 = """
<max_server_memory_usage_to_ram_ratio>0.95</max_server_memory_usage_to_ram_ratio>
<cache_size_to_ram_max_ratio>0.95</cache_size_to_ram_max_ratio>
<vector_similarity_index_cache_size>214748364800</vector_similarity_index_cache_size>
<max_build_vector_similarity_index_thread_pool_size>48</max_build_vector_similarity_index_thread_pool_size>
<vector_similarity_index_cache_size_ratio>0.99</vector_similarity_index_cache_size_ratio>
</clickhouse>
        """
        commands = [f'sed -i "s|</clickhouse>||g" {temp_dir}/config.xml']
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)

        with open(f"{temp_dir}/config.xml", "a") as config_file:
            config_file.write(c1)
        return res

    def create_log_export_config(self):
        print("Create log export config")
        config_file = Path(self.ch_config_dir) / "config.d" / "system_logs_export.yaml"
        config_file.parent.mkdir(parents=True, exist_ok=True)

        self.log_export_host, self.log_export_password = (
            Secret.Config(
                name="clickhouse_ci_logs_host",
                type=Secret.Type.AWS_SSM_PARAMETER,
                region="us-east-1",
            )
            .join_with(
                Secret.Config(
                    name="clickhouse_ci_logs_password",
                    type=Secret.Type.AWS_SSM_PARAMETER,
                    region="us-east-1",
                )
            )
            .get_value()
        )

        config_content = LOG_EXPORT_CONFIG_TEMPLATE.format(
            CLICKHOUSE_CI_LOGS_CLUSTER=CLICKHOUSE_CI_LOGS_CLUSTER,
            CLICKHOUSE_CI_LOGS_HOST=self.log_export_host,
            CLICKHOUSE_CI_LOGS_USER=CLICKHOUSE_CI_LOGS_USER,
            CLICKHOUSE_CI_LOGS_PASSWORD=self.log_export_password,
        )

        with open(config_file, "w") as f:
            f.write(config_content)
        return True

    def start_log_exports(self, check_start_time):
        print("Start log export")
        if self.log_export_host:
            os.environ["CLICKHOUSE_CI_LOGS_CLUSTER"] = CLICKHOUSE_CI_LOGS_CLUSTER
            os.environ["CLICKHOUSE_CI_LOGS_HOST"] = self.log_export_host
            os.environ["CLICKHOUSE_CI_LOGS_USER"] = CLICKHOUSE_CI_LOGS_USER
            os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = self.log_export_password
        info = Info()
        os.environ["EXTRA_COLUMNS_EXPRESSION"] = (
            f"toLowCardinality('{info.repo_name}') AS repo, CAST({info.pr_number} AS UInt32) AS pull_request_number, '{info.sha}' AS commit_sha, toDateTime('{Utils.timestamp_to_str(check_start_time)}', 'UTC') AS check_start_time, toLowCardinality('{info.job_name}') AS check_name, toLowCardinality('{info.instance_type}') AS instance_type, '{info.instance_id}' AS instance_id"
        )

        return Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication",
            verbose=True,
        )

    @staticmethod
    def stop_log_exports():
        return Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
            verbose=True,
        )

    def start(self, replica_num=0):
        if replica_num == 0:
            # Clear dmesg to avoid false OOM detection from previous CI jobs on the same host
            Shell.check("dmesg --clear", verbose=True)
            kill_leftover_server_processes()

        if replica_num == 1:
            pid_file = self.pid_file_replica_1
            command = self.replica_command_1
            run_path = self.run_path1
        elif replica_num == 2:
            pid_file = self.pid_file_replica_2
            command = self.replica_command_2
            run_path = self.run_path2
        elif replica_num == 0:
            pid_file = self.pid_file
            command = self.command
            run_path = self.run_path0
        else:
            assert False

        print(f"Starting ClickHouse server replica {replica_num}, command: {command}")

        Path(pid_file).unlink(missing_ok=True)
        Utils.clean_dir(Path(run_path))
        Utils.clean_dir(p_temp_dir / "jemalloc_profiles")

        replicas = 3 if self.is_db_replicated else 1
        tsan_memory_limit_mb = (
            Utils.physical_memory() * 65 // 100 // 1024 // 1024 // replicas
        )

        # set profile file for the server (not needed for per-test coverage,
        # which uses system.coverage_log instead of .profraw files)
        if not self.is_per_test_coverage:
            os.environ["LLVM_PROFILE_FILE"] = "ft-server-%m.profraw"

        env = os.environ.copy()
        env["TSAN_OPTIONS"] = " ".join(
            filter(
                lambda x: x is not None,
                [
                    env.get("TSAN_OPTIONS", None),
                    f"memory_limit_mb={tsan_memory_limit_mb}",
                ],
            )
        )
        tsan_options = env["TSAN_OPTIONS"]
        print(f"TSAN_OPTIONS = {tsan_options}")
        proc = subprocess.Popen(
            command,
            stderr=subprocess.STDOUT,
            shell=True,
            cwd=run_path,
            env=env,
        )
        if replica_num == 1:
            self.proc_1 = proc
        elif replica_num == 2:
            self.proc_2 = proc
        elif replica_num == 0:
            self.proc = proc
        else:
            assert False
        started = False
        try:
            for _ in range(15):
                pid = Shell.get_output(f"cat {pid_file}").strip()
                if not pid:
                    Utils.sleep(1)
                    continue
                started = True
                print(f"Got pid from fs [{pid}]")
                if replica_num == 1:
                    self.pid_1 = int(pid)
                elif replica_num == 2:
                    self.pid_2 = int(pid)
                elif replica_num == 0:
                    self.pid_0 = int(pid)
                else:
                    assert False
                break
        except Exception:
            pass

        if not started:
            stdout = proc.stdout.read().strip() if proc.stdout else ""
            stderr = proc.stderr.read().strip() if proc.stderr else ""
            Utils.print_formatted_error(
                f"Failed to start ClickHouse replica {replica_num}", stdout, stderr
            )
            return False

        print(
            f"ClickHouse server replica {replica_num} started successfully, pid [{pid}]"
        )
        res = True
        if self.is_db_replicated and replica_num == 0:
            res = self.start(replica_num=1) and self.start(replica_num=2)

        # System logs are flushed in wait_ready() once the server is listening,
        # not here: start()'s callers run wait_ready() afterwards, so a flush here
        # races the TCP listener and fails with Code 210 (Connection refused).
        self.save_system_metadata_files_from_remote_database_disk()

        return res

    def create_minio_log_tables(self):
        self.minio_setup_error = None
        # Minio log setup is non-fatal (caller continues when this returns
        # False). Every step MUST stay non-strict: a strict=True step would
        # raise before we can record the reason and signal failure. Record the
        # concrete failing sub-step so it reaches CIDB test_context_raw.
        # storage_policy = 'default' pins these diagnostic tables to local disk.
        # On s3 storage runs the default merge_tree policy is S3
        # (s3_storage_policy_for_merge_tree_by_default.xml), which would put the
        # audit log on S3, so (1) every audit-event insert writes parts to S3 and
        # thereby generates more audit events (a feedback loop that inflates the
        # table), and (2) the post-run `select * ... into outfile` dump reads all
        # of it back from S3 - on amd_tsan this JSON-typed table grew to ~700k
        # rows / ~1.5 GB and the dump blew past the DUMP_SYSTEM_TABLE_TIMEOUT cap,
        # failing the "Scraping system tables" check. These are diagnostics ABOUT
        # S3 activity; there is no reason to store them ON S3. 'default' is a
        # local policy on every stateless config (no config remaps it), so this
        # is a no-op on non-s3 runs.
        setup_steps = [
            (
                "create system.minio_audit_logs table",
                'clickhouse-client --enable_json_type=1 --query "CREATE TABLE system.minio_audit_logs (log JSON(time DateTime64(9))) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy = \'default\'"',
            ),
            (
                "create system.minio_server_logs table",
                'clickhouse-client --enable_json_type=1 --query "CREATE TABLE system.minio_server_logs (log JSON(time DateTime64(9))) ENGINE = MergeTree ORDER BY tuple() SETTINGS storage_policy = \'default\'"',
            ),
            (
                "set clickminio logger_webhook config",
                '/mc admin config set clickminio logger_webhook:ch_server_webhook endpoint="http://localhost:8123/?async_insert=1&wait_for_async_insert=0&async_insert_busy_timeout_min_ms=5000&async_insert_busy_timeout_max_ms=5000&async_insert_max_query_number=1000&async_insert_max_data_size=10485760&date_time_input_format=best_effort&query=INSERT%20INTO%20system.minio_server_logs%20FORMAT%20JSONAsObject" queue_size=1000000 batch_size=500',
            ),
            (
                "set clickminio audit_webhook config",
                '/mc admin config set clickminio audit_webhook:ch_audit_webhook endpoint="http://localhost:8123/?async_insert=1&wait_for_async_insert=0&async_insert_busy_timeout_min_ms=5000&async_insert_busy_timeout_max_ms=5000&async_insert_max_query_number=1000&async_insert_max_data_size=10485760&date_time_input_format=best_effort&query=INSERT%20INTO%20system.minio_audit_logs%20FORMAT%20JSONAsObject" queue_size=1000000 batch_size=500',
            ),
        ]
        for what, command in setup_steps:
            if not Shell.check(command, verbose=True):
                self.minio_setup_error = f"failed to {what}"
                print(f"ERROR: Failed to {what}")
                return False

        return self._restart_minio_to_apply_config()

    def _wait_minio_ready(self, timeout_s):
        """Poll until the `test` bucket is listable, or `timeout_s` elapses.

        `mc ls` against a down MinIO fails fast (connection refused), so this
        polls at roughly one-second intervals.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if Shell.check("/mc ls clickminio/test", verbose=False):
                return True
            time.sleep(1)
        return False

    @staticmethod
    def _minio_binary():
        """Locate the `minio` binary the same way `setup_minio.sh` does.

        The stateless-test docker image ships it at `/minio`, and a local
        download (`download_minio`) writes it to `$TEMP_DIR` (== `temp_dir`).
        `setup_minio.sh` finds it via `PATH="/:.:$PATH"` after `cd "$TEMP_DIR"`,
        which prefers `/minio`; mirror that precedence here. The Python harness
        only adds `temp_dir` to `PATH`, not `/`, so a bare `minio` would not
        resolve to the docker binary - use an explicit path instead.
        """
        for path in ("/minio", f"{temp_dir}/minio"):
            if os.path.exists(path):
                return path
        return ""

    def _force_restart_minio(self, attempts=3, ready_timeout_s=60):
        """Kill any running MinIO and start a fresh instance from the same data
        directory, so it re-reads the webhook config written by `mc admin config
        set`.

        Retried because (a) a just-killed MinIO can still hold port 11111 for a
        moment, making the next `minio server` exit immediately, and (b) MinIO
        startup can take a while on a loaded sanitizer host. Each attempt kills,
        restarts, and waits for readiness; a dead or too-slow instance is simply
        killed and started again on the next iteration.

        Returns None on success, or a concrete failure reason so the caller can
        carry it into minio_setup_error (CIDB test_context_raw).
        """
        minio_bin = self._minio_binary()
        if not minio_bin:
            reason = (
                f"cannot find the minio binary (looked for /minio and {temp_dir}/minio)"
            )
            print(f"ERROR: {reason}; cannot restart MinIO")
            return reason
        # Start MinIO with the same root credentials `setup_minio.sh` used.
        # Otherwise it comes up with different root credentials and the
        # `clickminio` alias (clickhouse/clickhouse) can no longer authenticate,
        # so every readiness check below would fail for all retry attempts.
        # `setup_minio.sh` resolves these as `${MINIO_ROOT_USER:-clickhouse}`;
        # do the same so a custom value in the environment is honored too.
        minio_root_user = os.environ.get("MINIO_ROOT_USER", "clickhouse")
        minio_root_password = os.environ.get("MINIO_ROOT_PASSWORD", "clickhouse")
        for attempt in range(1, attempts + 1):
            Shell.check("pkill -9 -f 'minio server'", verbose=True)
            # Give the OS time to release port 11111 before rebinding.
            time.sleep(3)
            Shell.check(
                f"MINIO_ROOT_USER={minio_root_user} "
                f"MINIO_ROOT_PASSWORD={minio_root_password} "
                f"nohup {minio_bin} server --address :11111 {temp_dir}/minio_data "
                f">> {self.MINIO_LOG} 2>&1 &",
                verbose=True,
            )
            if self._wait_minio_ready(ready_timeout_s):
                return None
            print(
                f"WARNING: MinIO not ready within {ready_timeout_s}s after restart "
                f"(attempt {attempt}/{attempts})"
            )
        reason = (
            f"manual MinIO restart did not become ready within {ready_timeout_s}s "
            f"after {attempts} attempts"
        )
        print(f"ERROR: {reason}")
        return reason

    def _restart_minio_to_apply_config(self):
        # Restart minio so it picks up the webhook config set above. The clean
        # `mc admin service restart --wait` can hang forever (see #97647), so it
        # runs under a timeout and, on timeout, is killed by process group to
        # avoid orphans blocking communicate() (see #98466). Whenever the clean
        # restart does not cleanly report success with a servable bucket, fall
        # back to a manual, retried restart, which is the reliable path on
        # loaded CI hosts (the clean restart routinely exceeds the timeout there;
        # see the bugfix-validation env-setup flake in PR #108821).
        restart_timeout = 60
        # The clean-restart outcome that pushed us onto the manual fallback, so
        # the terminal reason names why the reliable path was even attempted.
        clean_restart_reason = "clean clickminio restart did not report a ready service"
        try:
            print(f"Restarting clickminio (timeout {restart_timeout}s)")
            proc = subprocess.Popen(
                [
                    "/mc",
                    "admin",
                    "service",
                    "restart",
                    "clickminio",
                    "--wait",
                    "--json",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            try:
                stdout, _ = proc.communicate(timeout=restart_timeout)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.communicate()
                raise
            try:
                status = json_module.loads(stdout).get("status", "")
            except (json_module.JSONDecodeError, AttributeError):
                status = stdout.strip()
            # `--wait` only guarantees the admin API is back, not that the
            # bucket is servable yet, so confirm readiness before trusting it.
            if "success" in status and self._wait_minio_ready(30):
                return True
            clean_restart_reason = (
                f"clean clickminio restart did not report a ready service, status: [{status}]"
            )
            print(f"WARNING: {clean_restart_reason}")
        except (subprocess.TimeoutExpired, OSError):
            clean_restart_reason = (
                f"clean clickminio restart timed out after {restart_timeout}s"
            )
            print(f"WARNING: {clean_restart_reason}")

        print("Falling back to a manual MinIO restart")
        manual_restart_reason = self._force_restart_minio()
        if manual_restart_reason is None:
            return True
        # Non-fatal, but record the reason so the caller can persist it into the
        # setup Result (CIDB test_context_raw) instead of leaving minio failures
        # in the opaque "Cannot start clickhouse-server" bucket. Carry both the
        # clean-restart status and the manual-restart failure, otherwise the real
        # reason stays print-only and collapses to a generic CIDB bucket.
        self.minio_setup_error = (
            f"failed to restart clickminio ({clean_restart_reason}; "
            f"manual restart: {manual_restart_reason})"
        )
        print(f"ERROR: Failed to restart clickminio: {self.minio_setup_error}")
        return False

    def wait_ready(self, replica_num=0):
        res, out, err = 0, "", ""
        # A debug or sanitizer server can legitimately take over a minute from
        # fork to listening; the loop below exits early on success and detects a
        # dead server via proc.poll(), so a generous deadline costs nothing.
        attempts = 60
        delay = 2
        if replica_num == 1:
            pid_file = self.pid_file_replica_1
            port = self.port_1
            proc = self.proc_1
            err_log = f"{self.log_dir}/clickhouse-server.err.1.log"
        elif replica_num == 2:
            pid_file = self.pid_file_replica_2
            port = self.port_2
            proc = self.proc_2
            err_log = f"{self.log_dir}/clickhouse-server.err.2.log"
        elif replica_num == 0:
            pid_file = self.pid_file
            port = self.port
            proc = self.proc
            err_log = f"{self.log_dir}/clickhouse-server.err.log"
        else:
            assert False
        i = 0
        self.pid = None
        while i < 30:
            # can take some time if decompressing
            try:
                self.pid = int(Shell.get_output(f"cat {pid_file}").strip())
                break
            except Exception:
                Utils.sleep(1)
            i += 1
        if self.pid is None:
            print(f"Failed to get pid from fs [{pid_file}]")
            return False
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {port} --receive_timeout=5 --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print(f"Server replica {replica_num} ready")
                break
            else:
                print(f"Server replica {replica_num} not ready, err: {err}, wait")
            Utils.sleep(delay)
            status = proc.poll()
            if status is not None:
                print(f"Server replica {replica_num} (pid={proc.pid}) exited: {status}")
                Shell.check(f"echo 'Error log:' && tail -n100 {err_log}", verbose=True)
                return False
        else:
            Utils.print_formatted_error(
                f"Server replica {replica_num} not ready after [{attempts * delay}s]",
                out,
                err,
            )
            return False
        if self.is_db_replicated and replica_num == 0:
            if not (self.wait_ready(replica_num=1) and self.wait_ready(replica_num=2)):
                return False
        if replica_num == 0:
            # _flush_system_logs() fans out to every running replica; the
            # replica_num == 0 guard only stops the recursive wait_ready(1/2)
            # calls above from re-running that fan-out (replicas 1 and 2 are
            # already confirmed ready there). Flushing pre-creates the system log
            # tables so tests don't hit "table does not exist". Kept here, not in
            # start(), to avoid the Code 210 race against the TCP listener.
            self._flush_system_logs()
        return True

    def _flush_system_logs(self):
        for proc, port in zip(
            (self.proc, self.proc_1, self.proc_2), (self.port, self.port_1, self.port_2)
        ):
            if proc:
                res = Shell.check(
                    f'clickhouse-client --port {port} --query "system flush logs"',
                    verbose=True,
                )
                Shell.check(
                    f'clickhouse-client --port {port} --query "SYSTEM FLUSH ASYNC INSERT QUEUE"',
                    verbose=True,
                )
                if not res:
                    return False
        return True

    def prepare_stateful_data(self, with_s3_storage, is_db_replicated, build_type=None):
        self.stateful_setup_error = None
        if is_db_replicated:
            print("Skip stateful data preparation for db replicated")
            return True
        # Fewer insert threads on sanitizer binaries: their baseline RSS sits
        # near max_server_memory_usage, so 16 parallel insert pipelines trip the
        # total limit (Code 241). Same data is loaded, just a smaller peak.
        is_sanitizer = build_type is not None and any(
            san in build_type for san in ("asan", "tsan", "msan", "ubsan")
        )
        max_insert_threads = 4 if is_sanitizer else 16
        command = """
set -e
set -o pipefail
# Record which sub-command failed (set -e then exits). $BASH_COMMAND is the
# failing command itself, so the captured reason names the exact query instead
# of just a line number; combined with the ClickHouse client error already on
# stderr this is captured below so the bugfix-validation re-prepare path can
# report the real reason.
trap 'rc=$?; echo "prepare_stateful_data: command [$BASH_COMMAND] at line $LINENO failed with exit $rc" >&2' ERR

MAX_EXECUTION_TIME=1800

clickhouse-client --query "SHOW DATABASES"
clickhouse-client --query "CREATE DATABASE datasets"
clickhouse-client < ./tests/docker_scripts/create.sql
bash ./tests/docker_scripts/create_tpcds.sh
bash ./tests/docker_scripts/create_tpch.sh
clickhouse-client --query "SHOW TABLES FROM datasets"
clickhouse-client --query "SHOW TABLES FROM tpcds"
clickhouse-client --query "SHOW TABLES FROM tpch"

clickhouse-client --query "CREATE DATABASE test"
clickhouse-client --query "SHOW TABLES FROM test"
if [[ -n "$USE_S3_STORAGE_FOR_MERGE_TREE" ]] && [[ "$USE_S3_STORAGE_FOR_MERGE_TREE" -eq 1 ]]; then
    clickhouse-client --query "CREATE TABLE test.hits (WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16, EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32, UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String, RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8, MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16, ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32, SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8, FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32, HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String, HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32, FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32, LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String, OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String, UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64, URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String, ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64), IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8)
        ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate)
        ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
    clickhouse-client --query "CREATE TABLE test.visits (CounterID UInt32,  StartDate Date,  Sign Int8,  IsNew UInt8, VisitID UInt64,  UserID UInt64,  StartTime DateTime,  Duration UInt32,  UTCStartTime DateTime,  PageViews Int32, Hits Int32,  IsBounce UInt8,  Referer String,  StartURL String,  RefererDomain String,  StartURLDomain String, EndURL String,  LinkURL String,  IsDownload UInt8,  TraficSourceID Int8,  SearchEngineID UInt16,  SearchPhrase String, AdvEngineID UInt8,  PlaceID Int32,  RefererCategories Array(UInt16),  URLCategories Array(UInt16),  URLRegions Array(UInt32), RefererRegions Array(UInt32),  IsYandex UInt8,  GoalReachesDepth Int32,  GoalReachesURL Int32,  GoalReachesAny Int32, SocialSourceNetworkID UInt8,  SocialSourcePage String,  MobilePhoneModel String,  ClientEventTime DateTime,  RegionID UInt32, ClientIP UInt32,  ClientIP6 FixedString(16),  RemoteIP UInt32,  RemoteIP6 FixedString(16),  IPNetworkID UInt32, SilverlightVersion3 UInt32,  CodeVersion UInt32,  ResolutionWidth UInt16,  ResolutionHeight UInt16,  UserAgentMajor UInt16, UserAgentMinor UInt16,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  SilverlightVersion2 UInt8,  SilverlightVersion4 UInt16, FlashVersion3 UInt16,  FlashVersion4 UInt16,  ClientTimeZone Int16,  OS UInt8,  UserAgent UInt8,  ResolutionDepth UInt8, FlashMajor UInt8,  FlashMinor UInt8,  NetMajor UInt8,  NetMinor UInt8,  MobilePhone UInt8,  SilverlightVersion1 UInt8, Age UInt8,  Sex UInt8,  Income UInt8,  JavaEnable UInt8,  CookieEnable UInt8,  JavascriptEnable UInt8,  IsMobile UInt8, BrowserLanguage UInt16,  BrowserCountry UInt16,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), Params Array(String),  Goals Nested(ID UInt32, Serial UInt32, EventTime DateTime,  Price Int64,  OrderID String, CurrencyID UInt32), WatchIDs Array(UInt64),  ParamSumPrice Int64,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16,  ClickLogID UInt64, ClickEventID Int32,  ClickGoodEvent Int32,  ClickEventTime DateTime,  ClickPriorityID Int32,  ClickPhraseID Int32,  ClickPageID Int32, ClickPlaceID Int32,  ClickTypeID Int32,  ClickResourceID Int32,  ClickCost UInt32,  ClickClientIP UInt32,  ClickDomainID UInt32, ClickURL String,  ClickAttempt UInt8,  ClickOrderID UInt32,  ClickBannerID UInt32,  ClickMarketCategoryID UInt32,  ClickMarketPP UInt32, ClickMarketCategoryName String,  ClickMarketPPName String,  ClickAWAPSCampaignName String,  ClickPageName String,  ClickTargetType UInt16, ClickTargetPhraseID UInt64,  ClickContextType UInt8,  ClickSelectType Int8,  ClickOptions String,  ClickGroupBannerID Int32, OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String,  FromTag String,  HasGCLID UInt8,  FirstVisit DateTime, PredLastVisit Date,  LastVisit Date,  TotalVisits UInt32,  TraficSource    Nested(ID Int8,  SearchEngineID UInt16, AdvEngineID UInt8, PlaceID UInt16, SocialSourceNetworkID UInt8, Domain String, SearchPhrase String, SocialSourcePage String),  Attendance FixedString(16), CLID UInt32,  YCLID UInt64,  NormalizedRefererHash UInt64,  SearchPhraseHash UInt64,  RefererDomainHash UInt64,  NormalizedStartURLHash UInt64, StartURLDomainHash UInt64,  NormalizedEndURLHash UInt64,  TopLevelDomain UInt64,  URLScheme UInt64,  OpenstatServiceNameHash UInt64, OpenstatCampaignIDHash UInt64,  OpenstatAdIDHash UInt64,  OpenstatSourceIDHash UInt64,  UTMSourceHash UInt64,  UTMMediumHash UInt64, UTMCampaignHash UInt64,  UTMContentHash UInt64,  UTMTermHash UInt64,  FromHash UInt64,  WebVisorEnabled UInt8,  WebVisorActivity UInt32, ParsedParams    Nested(Key1 String,  Key2 String,  Key3 String,  Key4 String, Key5 String, ValueDouble    Float64), Market Nested(Type UInt8, GoalID UInt32, OrderID String,  OrderPrice Int64,  PP UInt32,  DirectPlaceID UInt32,  DirectOrderID  UInt32, DirectBannerID UInt32,  GoodID String, GoodName String, GoodQuantity Int32,  GoodPrice Int64),  IslandID FixedString(16))
        ENGINE = CollapsingMergeTree(Sign) PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
        SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"

    clickhouse-client --max_estimated_execution_time 0 --max_execution_time "$MAX_EXECUTION_TIME" --max_memory_usage 25G --query "INSERT INTO test.hits SELECT * FROM datasets.hits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=$MAX_INSERT_THREADS"
    clickhouse-client --max_estimated_execution_time 0 --max_execution_time "$MAX_EXECUTION_TIME" --max_memory_usage 25G --query "INSERT INTO test.visits SELECT * FROM datasets.visits_v1 SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=$MAX_INSERT_THREADS"
    clickhouse-client --query "DROP TABLE datasets.visits_v1 SYNC"
    clickhouse-client --query "DROP TABLE datasets.hits_v1 SYNC"
    # Note: `tpcds` and `tpch` databases are NOT dropped here as they are used by stateful tests.
else
    clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits"
    clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits"
fi
clickhouse-client --query "CREATE TABLE test.hits_s3  (WatchID UInt64, JavaEnable UInt8, Title String, GoodEvent Int16, EventTime DateTime, EventDate Date, CounterID UInt32, ClientIP UInt32, ClientIP6 FixedString(16), RegionID UInt32, UserID UInt64, CounterClass Int8, OS UInt8, UserAgent UInt8, URL String, Referer String, URLDomain String, RefererDomain String, Refresh UInt8, IsRobot UInt8, RefererCategories Array(UInt16), URLCategories Array(UInt16), URLRegions Array(UInt32), RefererRegions Array(UInt32), ResolutionWidth UInt16, ResolutionHeight UInt16, ResolutionDepth UInt8, FlashMajor UInt8, FlashMinor UInt8, FlashMinor2 String, NetMajor UInt8, NetMinor UInt8, UserAgentMajor UInt16, UserAgentMinor FixedString(2), CookieEnable UInt8, JavascriptEnable UInt8, IsMobile UInt8, MobilePhone UInt8, MobilePhoneModel String, Params String, IPNetworkID UInt32, TraficSourceID Int8, SearchEngineID UInt16, SearchPhrase String, AdvEngineID UInt8, IsArtifical UInt8, WindowClientWidth UInt16, WindowClientHeight UInt16, ClientTimeZone Int16, ClientEventTime DateTime, SilverlightVersion1 UInt8, SilverlightVersion2 UInt8, SilverlightVersion3 UInt32, SilverlightVersion4 UInt16, PageCharset String, CodeVersion UInt32, IsLink UInt8, IsDownload UInt8, IsNotBounce UInt8, FUniqID UInt64, HID UInt32, IsOldCounter UInt8, IsEvent UInt8, IsParameter UInt8, DontCountHits UInt8, WithHash UInt8, HitColor FixedString(1), UTCEventTime DateTime, Age UInt8, Sex UInt8, Income UInt8, Interests UInt16, Robotness UInt8, GeneralInterests Array(UInt16), RemoteIP UInt32, RemoteIP6 FixedString(16), WindowName Int32, OpenerName Int32, HistoryLength Int16, BrowserLanguage FixedString(2), BrowserCountry FixedString(2), SocialNetwork String, SocialAction String, HTTPError UInt16, SendTiming Int32, DNSTiming Int32, ConnectTiming Int32, ResponseStartTiming Int32, ResponseEndTiming Int32, FetchTiming Int32, RedirectTiming Int32, DOMInteractiveTiming Int32, DOMContentLoadedTiming Int32, DOMCompleteTiming Int32, LoadEventStartTiming Int32, LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32, FirstPaintTiming Int32, RedirectCount Int8, SocialSourceNetworkID UInt8, SocialSourcePage String, ParamPrice Int64, ParamOrderID String, ParamCurrency FixedString(3), ParamCurrencyID UInt16, GoalsReached Array(UInt32), OpenstatServiceName String, OpenstatCampaignID String, OpenstatAdID String, OpenstatSourceID String, UTMSource String, UTMMedium String, UTMCampaign String, UTMContent String, UTMTerm String, FromTag String, HasGCLID UInt8, RefererHash UInt64, URLHash UInt64, CLID UInt32, YCLID UInt64, ShareService String, ShareURL String, ShareTitle String, ParsedParams Nested(Key1 String, Key2 String, Key3 String, Key4 String, Key5 String, ValueDouble Float64), IslandID FixedString(16), RequestNum UInt32, RequestTry UInt8) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity = 8192, storage_policy='s3_cache'"
# AWS S3 is very inefficient, so increase memory even further:
clickhouse-client --max_estimated_execution_time 0 --max_execution_time "$MAX_EXECUTION_TIME" --max_memory_usage 30G --max_memory_usage_for_user 30G --query "INSERT INTO test.hits_s3 SELECT * FROM test.hits SETTINGS enable_filesystem_cache_on_write_operations=0, write_through_distributed_cache=0, max_insert_threads=$MAX_INSERT_THREADS"

clickhouse-client --query "CREATE TABLE test.hits_parquet (Title String, URL String, Referer String, SearchPhrase String, WatchID UInt64, UserID UInt64, CounterID UInt32, EventTime DateTime, EventDate Date, RegionID UInt32, ClientIP UInt32) ENGINE = S3('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/hits.parquet', NOSIGN)"

clickhouse-client --query "SHOW TABLES FROM test"
clickhouse-client --query "SELECT count() FROM test.hits"
clickhouse-client --query "SELECT count() FROM test.visits"
"""
        command = f"MAX_INSERT_THREADS={max_insert_threads}\n" + command
        if with_s3_storage:
            command = "USE_S3_STORAGE_FOR_MERGE_TREE=1\n" + command
        # Run via Shell.run (bash, like Shell.check) but keep a log file so that
        # on failure we can surface the failing sub-command + its ClickHouse
        # error tail to the caller. Same success semantics as before
        # (returncode == 0). This is what makes the intermittent msan re-prepare
        # failure diagnosable in CIDB instead of a generic boolean.
        log_file = f"{temp_dir}/prepare_stateful_data.log"
        rc = Shell.run(command, log_file=log_file, verbose=True)
        if rc != 0:
            tail = ""
            try:
                with open(log_file, errors="ignore") as f:
                    tail = "".join(f.readlines()[-15:]).strip()
            except OSError:
                pass
            self.stateful_setup_error = (
                f"stateful data prep failed (exit {rc})"
                + (f": {tail}" if tail else "")
            )
            print(f"ERROR: {self.stateful_setup_error}")
        return rc == 0

    def insert_system_zookeeper_config(self):
        for _ in range(10):
            res = Shell.check(
                f"clickhouse-client --query \"insert into system.zookeeper (name, path, value) values ('auxiliary_zookeeper2', '{temp_dir}/chroot/', '')\"",
                verbose=True,
            )
            time.sleep(1)
            if res:
                return True
        else:
            return False

    def run_test(self, cmd, timeout=7200):
        """Run a `clickhouse-test` command and return its integer exit code.

        Returns 0 on success, non-zero on failure. In particular, exit code
        `STOP_TESTING_EXIT_CODE` (2) signals that `clickhouse-test` aborted
        the run via `StopTesting` (server died, hung check failed, etc.) and
        is forwarded to `FTResultsProcessor.run` as `runner_exit_code` so it
        can populate the synthetic "Server died" leaf.
        """
        print(f"Run test: [{cmd}]")
        with open(self.test_output_file, "w") as f:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,  # line-buffered
                shell=True,
                text=True,
                errors="ignore",
                start_new_session=True,
            )

            def _reader():
                for line in process.stdout:
                    print(line, end="")
                    f.write(line)

            reader_thread = threading.Thread(target=_reader)
            reader_thread.start()

            try:
                process.wait(timeout=timeout)
                reader_thread.join()
                return process.returncode
            except subprocess.TimeoutExpired:
                print(
                    f"ERROR: fast test timed out after {timeout}s, killing process group"
                )
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                process.wait()
                reader_thread.join()
                return process.returncode
            finally:
                # Kill any test processes that survived clickhouse-test's own cleanup
                # (e.g. if it was killed with SIGKILL before its signal handlers ran).
                # clickhouse-test writes the group pid file itself on startup; --cleanup
                # reads it and kills all orphaned test process groups.
                _clickhouse_test = Path(__file__).resolve().parent.parent.parent.parent / "tests" / "clickhouse-test"
                subprocess.run([sys.executable, str(_clickhouse_test), "--cleanup"], check=False)

    def terminate(self, force=False):
        if self.minio_proc:
            # remove the webhook so it doesn't spam with errors once we stop ClickHouse
            Shell.check(
                "/mc admin config reset clickminio logger_webhook:ch_server_webhook",
                verbose=True,
            )
            Shell.check(
                "/mc admin config reset clickminio audit_webhook:ch_audit_webhook",
                verbose=True,
            )

        if self.kafka_proc:
            print("Stopping Redpanda broker")
            Shell.check("pkill -f redpanda", verbose=True)
            try:
                self.kafka_proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.kafka_proc.kill()

        self._flush_system_logs()

        self.save_system_metadata_files_from_remote_database_disk()

        self.stop_server(force=force)

        return self

    def stop_server(self, force=False):
        """Gracefully stop only the ClickHouse server processes.

        Unlike `terminate`, this leaves the auxiliary services (Redpanda/Kafka,
        MinIO and its webhooks) running. It is used between bugfix-validation
        iterations so the server binary can be swapped and restarted without
        tearing down the rest of the test environment: otherwise a changed test
        relying on Kafka or MinIO webhooks would pass under the first build type
        and spuriously "reproduce" a bug under the next one.
        """
        print("Stop ClickHouse processes")

        Shell.check("ps -ef | grep  clickhouse")
        for proc, pid_file, pid, run_path in (
            (self.proc, self.pid_file, self.pid_0, self.run_path0),
            (self.proc_1, self.pid_file_replica_1, self.pid_1, self.run_path1),
            (self.proc_2, self.pid_file_replica_2, self.pid_2, self.run_path2),
        ):
            if proc and pid:
                if force:
                    # Use clickhouse stop --force when this issue is fixed
                    # https://github.com/ClickHouse/ClickHouse/issues/99142
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                        continue
                    except subprocess.TimeoutExpired:
                        pass
                elif Shell.check(
                    f"cd {run_path} && clickhouse stop --pid-path {Path(pid_file).parent} --max-tries 300 --do-not-kill >/dev/null",
                    verbose=True,
                ):
                    continue
                print(
                    f"Failed to stop ClickHouse process {pid} gracefully - send TRAP signal to generate core file"
                )
                proc.send_signal(signal.SIGTRAP)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()

        return self

    def clean_logs(self):
        """
        Remove server logs from `log_dir`.

        Used between bugfix validation iterations to keep logs from different
        build types from being mixed together.
        """
        Utils.clean_dir(Path(self.log_dir))
        return self

    @staticmethod
    def _chmod(files):
        for file in files:
            try:
                os.chmod(file, 0o666)
            except Exception as ex:
                print(f"WARNING: Failed to chmod {file}: {ex}")

    def prepare_logs(self, info, all=False):
        res = []
        try:
            res = self._get_logs_archives_server()
            res += self._get_jemalloc_profiles()
            if all:
                res += self.debug_artifacts
                res += self.dump_system_tables()
                res += self._collect_core_dumps()
                res += self._collect_diagnostic_reports()
                res += self._get_logs_archive_coordination()
                if Path(self.MINIO_LOG).exists():
                    res.append(self.MINIO_LOG)
                if Path(self.AZURITE_LOG).exists():
                    res.append(self.AZURITE_LOG)
                if Path(self.KAFKA_LOG).exists():
                    res.append(self.KAFKA_LOG)
                if Path(self.DMESG_LOG).exists():
                    res.append(self.DMESG_LOG)
                if Path(self.CH_LOCAL_ERR_LOG).exists():
                    res.append(self.CH_LOCAL_ERR_LOG)
                if Path(self.CH_LOCAL_LOG).exists():
                    res.append(self.CH_LOCAL_LOG)
            self.logs = res
            self._chmod(self.logs)
        except Exception as e:
            print(f"WARNING: Failed to collect logs: {e}")
            traceback.print_exc()
            info.add_workflow_warning(
                f"Failed to collect all logs, ex [{e}], see job.log"
            )
        return res

    def _collect_core_dumps(self) -> List[str]:
        result = []
        for run_dir in sorted(p_temp_dir.glob("run_r*")):
            result.extend(ClickHouseService.collect_cores(run_dir))
        return result

    @staticmethod
    def _collect_diagnostic_reports() -> List[str]:
        # macOS writes .ips crash reports to /Library/Logs/DiagnosticReports as
        # root. Grant read access so the runner can list and read the files
        # in place; the darwin fast-test pre-hook wipes the directory under
        # sudo before the run, so anything we see here belongs to the current
        # run even if the previous runner was terminated unexpectedly.
        if platform.system() != "Darwin":
            return []
        reports_dir = Path("/Library/Logs/DiagnosticReports")
        Shell.check(
            f"sudo chmod -R a+rX {reports_dir}",
            verbose=True,
        )
        return [str(p) for p in reports_dir.glob("*.ips")]

    @classmethod
    def _get_logs_archive_coordination(cls):
        Shell.check(
            f"cd {temp_dir} && tar -czf coordination.tar.gz --files-from <(find . -type d -name coordination)",
            verbose=True,
        )
        if Path(f"{temp_dir}/coordination.tar.gz").exists():
            return [f"{temp_dir}/coordination.tar.gz"]
        else:
            print("WARNING: Coordination logs not found")
            return []

    @classmethod
    def _get_jemalloc_profiles(cls):
        profiles = Shell.get_output(f"ls {temp_dir}/jemalloc_profiles")
        if not profiles:
            return []

        profiles = profiles.split("\n")

        res = []

        # We will generate flamegraphs for last jemalloc profile of each PID
        # format of jemalloc profile: clickhouse.jemalloc.$PID.$COUNT.m$COUNT.heap
        # test runs can generate jemalloc profiles for multiple PIDs because clickhouse local and multiple servers
        # can be started

        # group profiles by pid
        grouped_profiles = defaultdict(list)
        for profile in profiles:
            parts = profile.split(".")
            pid = int(parts[2])
            count = int(parts[3])
            grouped_profiles[pid].append((count, profile))

        # for each group, get the file with the highest count number
        latest_profiles = {}
        for pid, files_in_group in grouped_profiles.items():
            file_with_max_third_number = max(files_in_group, key=lambda x: x[0])[1]
            latest_profiles[pid] = file_with_max_third_number

        chbinary = Shell.get_output("readlink -f $(which clickhouse)")
        for pid, profile in latest_profiles.items():
            Shell.check(
                f"jeprof {chbinary} {temp_dir}/jemalloc_profiles/{profile} --text > {temp_dir}/jemalloc_profiles/jemalloc.{pid}.txt 2>/dev/null",
                verbose=True,
            )
            Shell.check(
                f"jeprof {chbinary} {temp_dir}/jemalloc_profiles/{profile} --collapsed 2>/dev/null | flamegraph.pl --color mem --width 2560 > {temp_dir}/jemalloc_profiles/jemalloc.{pid}.svg",
                verbose=True,
            )

        Shell.check(
            f"cd {temp_dir} && tar -czf jemalloc.tar.zst --files-from <(find . -type d -name jemalloc_profiles)",
            verbose=True,
        )
        if Path(f"{temp_dir}/jemalloc.tar.zst").exists():
            res.append(f"{temp_dir}/jemalloc.tar.zst")
        else:
            print("WARNING: Jemalloc profiles not found")
            return []
        return res

    def _get_logs_archives_server(self):
        assert Path(
            self.log_dir
        ).exists(), f"Log directory {self.log_dir} does not exist"
        return [f for f in glob.glob(f"{self.log_dir}/*.log")]

    def check_ch_is_oom_killed(self):
        if Shell.check(f"dmesg > {self.DMESG_LOG}"):
            return Result.from_commands_run(
                name="OOM in dmesg",
                command=f"! cat {self.DMESG_LOG} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
            )
        else:
            return None

    def check_fatal_messages_in_logs(self):
        results = []

        # if command exit code is 1 - it's failed test case, script output will be stored into test case info
        results.append(
            Result.from_commands_run(
                name="Exception in test runner",
                command=rf"! awk 'found && /^[^[:space:]]/ {{ print; exit }} /^Traceback \(most recent call last\):/ {{ found=1 }} found {{ print }}' {temp_dir}/job.log | head -n 100 | tee /dev/stderr | grep -q .",
            )
        )

        def pick_latest_file(pattern: str) -> Path | None:
            log_dir = Path(self.log_dir)
            candidates = list(log_dir.glob(pattern))
            candidates = [p for p in candidates if p.is_file()]
            if not candidates:
                return None
            return max(candidates, key=lambda p: p.stat().st_mtime)

        sanitizer_hits = Shell.get_output(
            f"sed -n '/.*anitizer/,${{p}}' {self.log_dir}/stderr*.log 2>/dev/null | "
            f'grep -a -v "ASan doesn\'t fully support makecontext/swapcontext functions" | '
            f'grep -a -v "ASan is ignoring requested __asan_handle_no_return" | '
            f'grep -a -v "False positive error reports may follow" | '
            f'grep -a -v "For details see https://github.com/google/sanitizers" | '
            "head -n 1 || true"
        )
        fatal_hits = Shell.get_output(
            f"cd {self.log_dir} && grep -a '<Fatal>' clickhouse-server*.log 2>/dev/null | head -n 1 || true"
        )
        if sanitizer_hits or fatal_hits:
            server_log = pick_latest_file(
                "clickhouse-server*.err.log"
            ) or pick_latest_file("clickhouse-server*.log")
            stderr_log = pick_latest_file("stderr*.log")
            if not (server_log or stderr_log):
                results.append(
                    Result.create_from(
                        name="Sanitizer assert or Fatal messages in server logs",
                        info="no server logs found",
                        status=Result.Status.FAIL,
                        labels=[Result.Label.BLOCKER],  # to explicitly block the merge
                    )
                )
            else:
                try:
                    log_parser = FuzzerLogParser(
                        server_log=str(server_log),
                        stderr_log=str(stderr_log),
                        fuzzer_log="",
                    )
                    name, description, files = log_parser.parse_failure()
                    results.append(
                        Result.create_from(
                            name=name,
                            info=description,
                            status=Result.Status.FAIL,
                            files=files,
                            labels=[
                                Result.Label.BLOCKER
                            ],  # to explicitly block the merge
                        )
                    )
                except Exception:
                    results.append(
                        Result.create_from(
                            name="Failed to parse sanitizer/fatal failure from server logs",
                            info=traceback.format_exc(),
                            status=Result.Status.FAIL,
                            labels=[
                                Result.Label.BLOCKER
                            ],  # to explicitly block the merge
                        )
                    )

        results.append(
            Result.from_commands_run(
                name="Lost s3 keys",
                command=f"cd {self.log_dir} && ! grep -a 'Code: 499.*The specified key does not exist' clickhouse-server*.log | grep -v -e 'a.myext' -e 'ReadBuffer is canceled by the exception' -e 'DistributedCacheTCPHandler' -e 'ReadBufferFromDistributedCache' -e 'ReadBufferFromS3' -e 'ReadBufferFromAzureBlobStorage' -e 'AsynchronousBoundedReadBuffer' -e 'caller id: None:DistribCache' | head -n100 | tee /dev/stderr | grep -q .",
            )
        )
        results.append(
            Result.from_commands_run(
                name="Lost forever for SharedMergeTree",
                command=f"cd {self.log_dir} && ! grep -a 'it is lost forever' clickhouse-server*.log | head -n100 | tee /dev/stderr | grep -q .",
            )
        )
        results.append(
            Result.from_commands_run(
                name="Lost forever for SharedMergeTree",
                command=f"cd {self.log_dir} && ! grep -a 'it is lost forever' clickhouse-server*.log | head -n100 | tee /dev/stderr | grep -q .",
            )
        )
        results.append(
            Result.from_commands_run(
                name="S3_ERROR No such key thrown (in clickhouse-server.log or clickhouse-server.err.log)",
                command=f"cd {self.log_dir} && ! grep -a 'Code: 499.*The specified key does not exist' clickhouse-server*.log | grep -v -e 'a.myext' -e 'ReadBuffer is canceled by the exception'  -e 'DistributedCacheTCPHandler' -e 'ReadBufferFromDistributedCache' -e 'ReadBufferFromS3' -e 'ReadBufferFromAzureBlobStorage' -e 'AsynchronousBoundedReadBuffer' -e 'caller id: None:DistribCache' | head -n100 | tee /dev/stderr | grep -q .",
            )
        )
        oom_check = self.check_ch_is_oom_killed()
        if oom_check is None:
            print("WARNING: dmesg not enabled")
        else:
            results.append(oom_check)
        # convert statuses to CH tests notation
        for result in results:
            if result.is_ok():
                result.set_status(Result.Status.OK)
            else:
                result.set_status(Result.Status.FAIL)
            # These are server-log / runner health checks, not test cases.
            # The bugfix-validation inverter uses this label so a clean check
            # (OK) is left as-is instead of being flipped into a spurious
            # failure. A failing check still flips like a test (a fatal on the
            # validated binary is the bug reproducing).
            result.set_label(Result.Label.LOG_CHECK)
        return results

    # Exit codes coreutils `timeout` uses on expiry: 124 when the child dies on
    # the initial SIGTERM, 128+9 = 137 when a SIGTERM-ignoring child is escalated
    # with SIGKILL after --kill-after. Both mean the dump exceeded its cap.
    _TIMEOUT_EXIT_CODES = (124, 137)

    def _annotate_timeout(self, res, stderr):
        # If `res` is one of timeout's expiry codes, prepend the "timed out"
        # annotation so a stuck dump is reported as a timeout rather than an
        # opaque non-zero failure. Returns the (possibly) annotated stderr.
        if res in self._TIMEOUT_EXIT_CODES:
            return f"timed out after {self.DUMP_SYSTEM_TABLE_TIMEOUT}s\n{stderr}"
        return stderr

    def dump_system_tables(self):
        # Stop server so we can safely read data with clickhouse-local.
        # Why do we read data with clickhouse-local?
        # Because it's the simplest way to read it when server has crashed.
        # Increase timeout to 10 minutes (max-tries * 2 seconds) to give gdb time to collect stack traces
        # (if safeExit breakpoint is hit after the server's internal shutdown timeout is reached).

        # # Remove all limits to avoid TOO_MANY_ROWS_OR_BYTES while gathering system.*_log tables
        # Shell.check("rm /etc/clickhouse-server/users.d/limits.yaml", verbose=True)
        # Shell.check("clickhouse-client -q \"system reload config\" ||:", verbose=True)
        TABLES = [
            "query_log",
            "zookeeper_log",
            "aggregated_zookeeper_log",
            "trace_log",
            "transactions_info_log",
            "metric_log",
            "blob_storage_log",
            "error_log",
            "query_metric_log",
            "part_log",
            "minio_audit_logs",
            "minio_server_logs",
        ]
        ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT = 20_000_000

        command_args = self.LOGS_SAVER_CLIENT_OPTIONS
        # command_args += f" --config-file={self.ch_config_dir}/config.xml"
        command_args += " --only-system-tables --stacktrace"
        # we need disk definitions for S3 configurations, but it is OK to always use server config

        command_args += " --config-file=/etc/clickhouse-server/config.xml"
        # Change log files for local in config.xml as command args do not override
        Shell.check(
            f"sed -i 's|<log>.*</log>|<log>{self.CH_LOCAL_LOG}</log>|' /etc/clickhouse-server/config.xml"
        )
        Shell.check(
            f"sed -i 's|<errorlog>.*</errorlog>|<errorlog>{self.CH_LOCAL_ERR_LOG}</errorlog>|' /etc/clickhouse-server/config.xml"
        )
        # FIXME: Hack for s3_with_keeper (note, that we don't need the disk,
        # the problem is that whenever we need disks all disks will be
        # initialized [1])
        #
        #   [1]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        #   [2]: https://github.com/ClickHouse/ClickHouse/issues/77320
        #
        command_args_post = "-- --zookeeper.implementation=testkeeper"

        # Bound each dump: a single hanging table (e.g. a huge minio_audit_logs
        # on s3 runs) must not consume the whole 9000s job budget. On expiry
        # timeout sends SIGTERM and returns 124; a dump that ignores SIGTERM is
        # escalated with SIGKILL after --kill-after and returns 128+9 = 137.
        # Both are timeouts, annotated by _annotate_timeout below.
        dump_prefix = f"timeout --signal=TERM --kill-after=60 {self.DUMP_SYSTEM_TABLE_TIMEOUT} "

        Utils.clean_dir(p_temp_dir / "system_tables")
        res = True

        self.restore_system_metadata_files_from_remote_database_disk()

        cache_status_files = glob.glob(
            f"{self.ch_var_lib_dir}/filesystem_caches/*/status"
        )
        if cache_status_files:
            print(
                f"WARNING: Server died? Removing cache status files: {cache_status_files}"
            )
            for cache_status_path in cache_status_files:
                Shell.check(f"rm {cache_status_path}", verbose=True)

        scraping_system_table = Result(name="Scraping system tables", status=Result.Status.OK)
        for table in TABLES:
            path_arg = f" --path {self.run_path0}"
            res, stdout, stderr = Shell.get_res_stdout_stderr(
                f"cd {self.run_path0} && {dump_prefix}clickhouse local {command_args} {path_arg} --query \"select * from system.{table} into outfile '{temp_dir}/system_tables/{table}.tsv' format TSVWithNamesAndTypes\" {command_args_post}",
                verbose=True,
            )
            if res != 0:
                stderr = self._annotate_timeout(res, stderr)
                print(f"ERROR: Failed to dump system table: {table}\nError: {stderr}")
                scraping_system_table.set_info(
                    f"Failed to dump system table: {table}\nError: {stderr}"
                )
            else:
                lines_count = int(
                    Shell.get_output_or_raise(
                        f"cd {self.run_path0} && wc -l < {temp_dir}/system_tables/{table}.tsv",
                        verbose=True,
                    ).strip()
                )
                if lines_count > ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT:
                    scraping_system_table.set_info(
                        f"System table {table} has too many rows {lines_count} > {ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT}"
                    )

            if "minio" in table:
                # minio tables are not replicated
                continue
            if self.is_shared_catalog or self.is_db_replicated:
                path_arg = f" --path {self.run_path1}"
                res, stdout, stderr = Shell.get_res_stdout_stderr(
                    f"cd {self.run_path1} && {dump_prefix}clickhouse local {command_args} {path_arg} --query \"select * from system.{table} into outfile '{temp_dir}/system_tables/{table}.1.tsv' format TSVWithNamesAndTypes\" {command_args_post}",
                    verbose=True,
                )
                if res != 0:
                    stderr = self._annotate_timeout(res, stderr)
                    print(
                        f"ERROR: Failed to dump system table from replica 1: {table}\nError: {stderr}"
                    )
                    scraping_system_table.set_info(
                        f"Failed to dump system table from replica 1: {table}\nError: {stderr}"
                    )
                    res = False
                else:
                    lines_count = int(
                        Shell.get_output_or_raise(
                            f"cd {self.run_path1} && wc -l < {temp_dir}/system_tables/{table}.1.tsv",
                            verbose=True,
                        ).strip()
                    )
                    if lines_count > ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT:
                        scraping_system_table.set_info(
                            f"System table {table} on replica 1 has too many rows {lines_count} > {ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT}"
                        )

            if self.is_db_replicated:
                path_arg = f" --path {self.run_path2}"
                res, stdout, stderr = Shell.get_res_stdout_stderr(
                    f"cd {self.run_path2} && {dump_prefix}clickhouse local {command_args} {path_arg} --query \"select * from system.{table} into outfile '{temp_dir}/system_tables/{table}.2.tsv' format TSVWithNamesAndTypes\" {command_args_post}",
                    verbose=True,
                )
                if res != 0:
                    stderr = self._annotate_timeout(res, stderr)
                    print(
                        f"ERROR: Failed to dump system table from replica 2: {table}\nError: {stderr}"
                    )
                    scraping_system_table.set_info(
                        f"Failed to dump system table from replica 2: {table}\nError: {stderr}"
                    )
                    res = False
                else:
                    lines_count = int(
                        Shell.get_output_or_raise(
                            f"cd {self.run_path2} && wc -l < {temp_dir}/system_tables/{table}.2.tsv",
                            verbose=True,
                        ).strip()
                    )
                    if lines_count > ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT:
                        scraping_system_table.set_info(
                            f"System table {table} on replica 2 has too many rows {lines_count} > {ROWS_COUNT_IN_SYSTEM_TABLE_LIMIT}"
                        )

        if scraping_system_table.info:
            scraping_system_table.set_status(Result.Status.FAIL)
            self.extra_tests_results.append(scraping_system_table)
        return [f for f in glob.glob(f"{temp_dir}/system_tables/*.tsv")]

    @staticmethod
    def is_valid_uuid(val):
        try:
            uuid_obj = uuid.UUID(val)
            return str(uuid_obj) == val.lower()
        except ValueError:
            return False

    def save_system_metadata_files_from_remote_database_disk(self):
        if not os.path.exists(
            "/etc/clickhouse-server/config.d/remote_database_disk.xml"
        ):
            return

        # Store system database and table metadata files
        system_db_uuid = Shell.get_output(
            "clickhouse disks -C /etc/clickhouse-server/config.xml --disk disk_db_remote -q 'read metadata/system.sql' | grep -F UUID | awk -F\"'\" '{print $2}'",
            verbose=True,
        )
        if not self.is_valid_uuid(system_db_uuid):
            print(f"invalid system_db_uuid: '{system_db_uuid}'")
            return

        if self.system_db_uuid != None and self.system_db_uuid != system_db_uuid:
            print(
                f"system_db_uuid changed: '{self.system_db_uuid}' -> '{system_db_uuid}'"
            )

        self.system_db_uuid = system_db_uuid
        self.system_db_sql = Shell.get_output(
            "clickhouse disks -C /etc/clickhouse-server/config.xml --disk disk_db_remote -q 'read metadata/system.sql'",
            verbose=True,
        )
        print(f"system_db_uuid = '{self.system_db_uuid}'")
        print(f"system_db_sql = '{self.system_db_sql}'")

        system_table_sql_files = (
            Shell.get_output(
                f"clickhouse disks -C /etc/clickhouse-server/config.xml --disk disk_db_remote -q 'ls store/{self.system_db_uuid[:3]}/{self.system_db_uuid}/'",
                verbose=True,
            )
            .strip()
            .split("\n")
        )
        self.system_table_sql_map = {}
        for system_table_sql_file in system_table_sql_files:
            print(f"system_table_sql_file = '{system_table_sql_file}'")
            sql_content = Shell.get_output(
                f"clickhouse disks -C /etc/clickhouse-server/config.xml --disk disk_db_remote -q 'read store/{self.system_db_uuid[:3]}/{self.system_db_uuid}/{system_table_sql_file}'",
                verbose=True,
            )
            self.system_table_sql_map[system_table_sql_file] = sql_content

    def restore_system_metadata_files_from_remote_database_disk(self):
        if self.system_db_uuid is None:
            return

        # Ensure no remote database disk config
        if os.path.exists("/etc/clickhouse-server/config.d/remote_database_disk.xml"):
            os.remove("/etc/clickhouse-server/config.d/remote_database_disk.xml")

        # Restore system database and table metadata files for `clickhouse local`
        with open(f"{self.run_path0}/metadata/system.sql", "w") as file:
            file.write(self.system_db_sql)
        Shell.check(
            f"mkdir -p {self.run_path0}/store/{self.system_db_uuid[:3]}/{self.system_db_uuid}",
            verbose=True,
        )
        for system_table_sql_file, content in self.system_table_sql_map.items():
            with open(
                f"{self.run_path0}/store/{self.system_db_uuid[:3]}/{self.system_db_uuid}/{system_table_sql_file}",
                "w",
            ) as file:
                file.write(content)

    @staticmethod
    def set_random_timezone():
        tz = Shell.get_output(
            "rg -v '#' /usr/share/zoneinfo/zone.tab  | awk '{print $3}' | shuf | head -n1"
        )
        print(f"Chosen random timezone: {tz}")
        assert tz, "Failed to get random TZ"
        Shell.check(
            f"cat /usr/share/zoneinfo/{tz} > /etc/localtime && echo '{tz}' > /etc/timezone",
            verbose=True,
            strict=True,
        )


if __name__ == "__main__":
    ch = ClickHouseProc()
    command = sys.argv[1]
    res = False
    try:
        if command == "logs_export_config":
            if not Info().is_local_run:
                # Disable log export for local runs - ideally this command wouldn't be triggered,
                # but conditional disabling is complex in legacy bash scripts (run_fuzzer.sh, stress_runner.sh)
                res = ch.create_log_export_config()
            else:
                res = True
        elif command == "logs_export_start":
            # FIXME: the start_time must be preserved globally in ENV or something like that
            # to get the same values in different DBs
            # As a wild idea, it could be stored in a Info.check_start_timestamp
            if not Info().is_local_run:
                # Disable log export for local runs - ideally this command wouldn't be triggered,
                # but conditional disabling is complex in legacy bash scripts (run_fuzzer.sh, stress_runner.sh)
                res = ch.start_log_exports(check_start_time=Utils.timestamp())
            else:
                res = True
        elif command == "logs_export_stop":
            if not Info().is_local_run:
                # Disable log export for local runs - ideally this command wouldn't be triggered,
                # but conditional disabling is complex in legacy bash scripts (run_fuzzer.sh, stress_runner.sh)
                res = ch.stop_log_exports()
            else:
                res = True
        elif command == "start_minio":
            param = sys.argv[2]
            assert param in ["stateless"]
            res = ch.start_minio(param)
        elif command == "start_azurite":
            res = ch.start_azurite()
        else:
            raise ValueError(f"Unknown command: {command}")
    except Exception:
        print(f"ERROR: Failed to do [{command}]")
        traceback.print_exc()

    sys.exit(1 if not res else 0)
