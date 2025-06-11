import os
import subprocess
import time
from pathlib import Path

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


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
    BACKUPS_XML = """
<clickhouse>
    <backups>
        <type>local</type>
        <path>{CH_RUNTIME_DIR}/var/lib/clickhouse/disks/backups/</path>
    </backups>
</clickhouse>
"""

    def __init__(self, fast_test=False):
        self.ch_config_dir = f"{temp_dir}/etc/clickhouse-server"
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.config_file = f"{self.ch_config_dir}/config.xml"
        self.user_files_path = f"{self.ch_config_dir}/user_files"
        self.test_output_file = f"{temp_dir}/test_result.txt"
        self.command = f"clickhouse-server --config-file {self.config_file} --pid-file {self.pid_file} -- --path {self.ch_config_dir} --user_files_path {self.user_files_path} --top_level_domains_path {self.ch_config_dir}/top_level_domains --keeper_server.storage_path {self.ch_config_dir}/coordination"
        self.proc = None
        self.pid = 0
        nproc = int(Utils.cpu_count() / 2)
        self.fast_test_command = f"clickhouse-test --hung-check --trace --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --zookeeper --check-zookeeper-session --order random --report-logs-stats --fast-tests-only --no-stateful --jobs {nproc} -- '{{TEST}}' | ts '%Y-%m-%d %H:%M:%S' \
        | tee -a \"{self.test_output_file}\""
        # TODO: store info in case of failure
        self.info = ""
        self.info_file = ""

        Utils.set_env("CLICKHOUSE_CONFIG_DIR", self.ch_config_dir)
        Utils.set_env("CLICKHOUSE_CONFIG", self.config_file)
        Utils.set_env("CLICKHOUSE_USER_FILES", self.user_files_path)
        # Utils.set_env("CLICKHOUSE_SCHEMA_FILES", f"{self.ch_config_dir}/format_schemas")

        # if not fast_test:
        #     with open(f"{self.ch_config_dir}/config.d/backups.xml", "w") as file:
        #         file.write(self.BACKUPS_XML)

        self.minio_proc = None

    def start_minio(self, test_type, log_file_path):
        os.environ["TEMP_DIR"] = f"{Utils.cwd()}/ci/tmp"
        command = [
            "./ci/jobs/scripts/functional_tests/setup_minio.sh",
            test_type,
            "./tests",
        ]
        with open(log_file_path, "w") as log_file:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT
            )
        print(f"Started setup_minio.sh asynchronously with PID {process.pid}")
        return True

    @staticmethod
    def log_cluster_config():
        return Shell.check(
            f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --config-logs-export-cluster ./tmp_ci/etc/clickhouse-server/config.d/system_logs_export.yaml",
            verbose=True,
        )

    @staticmethod
    def log_cluster_setup_replication():
        return Shell.check(
            f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication",
            verbose=True,
        )

    @staticmethod
    def log_cluster_stop_replication():
        return Shell.check(
            f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
            verbose=True,
        )

    def start(self):
        print("Starting ClickHouse server")
        Shell.check(f"rm {self.pid_file}")
        self.proc = subprocess.Popen(self.command, stderr=subprocess.STDOUT, shell=True)
        started = False
        try:
            for _ in range(5):
                pid = Shell.get_output(f"cat {self.pid_file}").strip()
                if not pid:
                    Utils.sleep(1)
                    continue
                started = True
                print(f"Got pid from fs [{pid}]")
                _ = int(pid)
                break
        except Exception:
            pass

        if not started:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False

        print(f"ClickHouse server started successfully, pid [{pid}]")
        return True

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                'clickhouse-client --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True

    def run_fast_test(self, test=""):
        if Path(self.test_output_file).exists():
            Path(self.test_output_file).unlink()
        exit_code = Shell.run(self.fast_test_command.format(TEST=test), verbose=True)
        return exit_code == 0

    def terminate(self):
        print("Terminate ClickHouse process")
        timeout = 10
        if self.proc:
            Utils.terminate_process_group(self.proc.pid)

            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
                print(f"Process {self.proc.pid} terminated gracefully.")
            except Exception:
                print(
                    f"Process {self.proc.pid} did not terminate in {timeout} seconds, killing it..."
                )
                Utils.terminate_process_group(self.proc.pid, force=True)
                self.proc.wait()  # Wait for the process to be fully killed
                print(f"Process {self.proc} was killed.")

        if self.minio_proc:
            Utils.terminate_process_group(self.minio_proc.pid)


class ClickHouseLight:
    def __init__(self):
        self.path = temp_dir
        self.config_path = f"{temp_dir}/config"
        self.pid_file = "/tmp/pid"
        self.start_cmd = f"{self.path}/clickhouse-server --config-file={self.config_path}/config.xml --pid-file {self.pid_file}"
        self.log_file = f"{temp_dir}/server.log"
        self.port = 9000
        self.pid = None

    def install(self):
        Utils.add_to_PATH(self.path)
        commands = [
            f"mkdir -p {self.config_path}/users.d",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {self.config_path}",
            # make it ipv4 only
            f'sed -i "s|<!-- <listen_host>0.0.0.0</listen_host> -->|<listen_host>0.0.0.0</listen_host>|" {self.config_path}/config.xml',
            f"cp -r --dereference ./programs/server/config.d {self.config_path}",
            f"chmod +x {self.path}/clickhouse",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-server",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-client",
        ]
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def clickbench_config_tweaks(self):
        content = """
profiles:
    default:
        allow_introspection_functions: 1
"""
        file_path = f"{self.config_path}/users.d/allow_introspection_functions.yaml"
        with open(file_path, "w") as file:
            file.write(content)
        return True

    def fuzzer_config_tweaks(self):
        # TODO figure out which ones are needed
        commands = [
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/query-fuzzer-tweaks-users.xml {self.config_path}/users.d",
            f"cp -av --dereference ./ci/jobs/scripts/fuzzer/allow-nullable-key.xml {self.config_path}/config.d",
        ]

        c1 = """
<clickhouse>
    <max_server_memory_usage_to_ram_ratio>0.75</max_server_memory_usage_to_ram_ratio>
</clickhouse>
"""
        c2 = """
<clickhouse>
    <core_dump>
        <!-- 100GiB -->
        <size_limit>107374182400</size_limit>
    </core_dump>
    <!-- NOTE: no need to configure core_path,
    since clickhouse is not started as daemon (via clickhouse start)
    -->
    <core_path>$PWD</core_path>
</clickhouse>
"""
        file_path = (
            f"{self.config_path}/config.d/max_server_memory_usage_to_ram_ratio.xml"
        )
        with open(file_path, "w") as file:
            file.write(c1)

        file_path = f"{self.config_path}/config.d/core.xml"
        with open(file_path, "w") as file:
            file.write(c2)
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def create_log_export_config(self):
        print("Create log export config")
        config_file = Path(self.config_path) / "config.d" / "system_logs_export.yaml"

        self.log_export_host = Secret.Config(
            name="clickhouse_ci_logs_host",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        self.log_export_password = Secret.Config(
            name="clickhouse_ci_logs_password",
            type=Secret.Type.AWS_SSM_VAR,
            region="us-east-1",
        ).get_value()

        config_content = LOG_EXPORT_CONFIG_TEMPLATE.format(
            CLICKHOUSE_CI_LOGS_CLUSTER=CLICKHOUSE_CI_LOGS_CLUSTER,
            CLICKHOUSE_CI_LOGS_HOST=self.log_export_host,
            CLICKHOUSE_CI_LOGS_USER=CLICKHOUSE_CI_LOGS_USER,
            CLICKHOUSE_CI_LOGS_PASSWORD=self.log_export_password,
        )

        with open(config_file, "w") as f:
            f.write(config_content)

    def start_log_exports(self, check_start_time):
        print("Start log export")
        os.environ["CLICKHOUSE_CI_LOGS_CLUSTER"] = CLICKHOUSE_CI_LOGS_CLUSTER
        os.environ["CLICKHOUSE_CI_LOGS_HOST"] = self.log_export_host
        os.environ["CLICKHOUSE_CI_LOGS_USER"] = CLICKHOUSE_CI_LOGS_USER
        os.environ["CLICKHOUSE_CI_LOGS_PASSWORD"] = self.log_export_password
        info = Info()
        os.environ["EXTRA_COLUMNS_EXPRESSION"] = (
            f"CAST({info.pr_number} AS UInt32) AS pull_request_number, '{info.sha}' AS commit_sha, toDateTime('{Utils.timestamp_to_str(check_start_time)}', 'UTC') AS check_start_time, toLowCardinality('{info.job_name}') AS check_name, toLowCardinality('{info.instance_type}') AS instance_type, '{info.instance_id}' AS instance_id"
        )

        Shell.check(
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication",
            verbose=True,
            strict=True,
        )

    def start(self):
        print(f"Starting ClickHouse server")
        print("Command: ", self.start_cmd)
        self.log_fd = open(self.log_file, "w")
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
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        self.pid = int(Shell.get_output(f"cat {self.pid_file}").strip())
        return True

    def attach_gdb(self):
        assert self.pid, "ClickHouse not started"
        rtmin = Shell.get_output("kill -l SIGRTMIN", strict=True)
        script = f"""
    set follow-fork-mode parent
    handle SIGHUP nostop noprint pass
    handle SIGINT nostop noprint pass
    handle SIGQUIT nostop noprint pass
    handle SIGPIPE nostop noprint pass
    handle SIGTERM nostop noprint pass
    handle SIGUSR1 nostop noprint pass
    handle SIGUSR2 nostop noprint pass
    handle SIG{rtmin} nostop noprint pass
    info signals
    continue
    backtrace full
    thread apply all backtrace full
    info registers
    disassemble /s
    up
    disassemble /s
    up
    disassemble /s
    p "done"
    detach
    quit
"""
        script_path = f"./script.gdb"
        with open(script_path, "w") as file:
            file.write(script)

        self.proc = subprocess.Popen(
            f"gdb -batch -command {script_path} -p {self.pid}", shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to attaach gdb", stdout, stderr)
            return False

        # gdb will send SIGSTOP, spend some time loading debug info, and then send SIGCONT, wait for it (up to send_timeout, 300s)
        Shell.check(
            "time clickhouse-client --query \"SELECT 'Connected to clickhouse-server after attaching gdb'\"",
            verbose=True,
        )

        # Check connectivity after we attach gdb, because it might cause the server
        # to freeze, and the fuzzer will fail. In debug build, it can take a lot of time.
        res = self.wait_ready()
        if res:
            print("GDB attached successfully")
        return res
