import subprocess
from pathlib import Path

from praktika.settings import Settings
from praktika.utils import Shell, Utils


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
        self.ch_config_dir = f"{Settings.TEMP_DIR}/etc/clickhouse-server"
        self.pid_file = f"{self.ch_config_dir}/clickhouse-server.pid"
        self.config_file = f"{self.ch_config_dir}/config.xml"
        self.user_files_path = f"{self.ch_config_dir}/user_files"
        self.test_output_file = f"{Settings.OUTPUT_DIR}/test_result.txt"
        self.command = f"clickhouse-server --config-file {self.config_file} --pid-file {self.pid_file} -- --path {self.ch_config_dir} --user_files_path {self.user_files_path} --top_level_domains_path {self.ch_config_dir}/top_level_domains --keeper_server.storage_path {self.ch_config_dir}/coordination"
        self.proc = None
        self.pid = 0
        nproc = int(Utils.cpu_count() / 2)
        self.fast_test_command = f"clickhouse-test --hung-check --fast-tests-only --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --zookeeper --check-zookeeper-session --order random --print-time --report-logs-stats --jobs {nproc} -- '' | ts '%Y-%m-%d %H:%M:%S' \
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

    def start_hdfs(self, log_file_path):
        command = ["./ci/jobs/scripts/functional_tests/setup_hdfs_minicluster.sh"]
        with open(log_file_path, "w") as log_file:
            process = subprocess.Popen(
                command, stdout=log_file, stderr=subprocess.STDOUT
            )
        print(
            f"Started setup_hdfs_minicluster.sh asynchronously with PID {process.pid}"
        )
        return True

    def start_minio(self, test_type, log_file_path):
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

    def run_fast_test(self):
        if Path(self.test_output_file).exists():
            Path(self.test_output_file).unlink()
        exit_code = Shell.run(self.fast_test_command)
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
