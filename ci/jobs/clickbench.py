import subprocess
import time

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


# TODO: generic functionality - move to separate file
class ClickHouseBinary:
    def __init__(self):
        self.path = temp_dir
        self.config_path = f"{temp_dir}/config"
        self.start_cmd = (
            f"{self.path}/clickhouse-server --config-file={self.config_path}/config.xml"
        )
        self.log_file = f"{temp_dir}/server.log"
        self.port = 9000

    def install(self):
        Utils.add_to_PATH(self.path)
        commands = [
            f"mkdir -p {self.config_path}/users.d",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {self.config_path}",
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
        return True


def main():
    res = True
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseBinary()

    if res:
        print("Install ClickHouse")

        def install():
            return ch.install() and ch.clickbench_config_tweaks()

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=install)
        )
        res = results[-1].is_ok()

    if res:
        print("Start ClickHouse")

        def start():
            return ch.start()

        log_export_config = f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --config-logs-export-cluster {ch.config_path}/config.d/system_logs_export.yaml"
        setup_logs_replication = f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --setup-logs-replication"

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=[start, log_export_config, setup_logs_replication],
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Load the data")
        results.append(
            Result.from_commands_run(
                name="Load the data",
                command="clickhouse-client --time < ./ci/jobs/scripts/clickbench/create.sql",
            )
        )
        res = results[-1].is_ok()

    if res:
        print("Queries")
        stop_watch_ = Utils.Stopwatch()
        TRIES = 3
        QUERY_NUM = 1

        with open("./ci/jobs/scripts/clickbench/queries.sql", "r") as queries_file:
            query_results = []
            for query in queries_file:
                query = query.strip()
                timing = []

                for i in range(1, TRIES + 1):
                    query_id = f"q{QUERY_NUM}-{i}"
                    res, out, time_err = Shell.get_res_stdout_stderr(
                        f'clickhouse-client --query_id {query_id} --time --format Null --query "{query}" --progress 0',
                        verbose=True,
                    )
                    timing.append(time_err)
                    query_results.append(
                        Result(
                            name=f"{QUERY_NUM}_{i}",
                            status=Result.Status.SUCCESS,
                            duration=float(time_err),
                        )
                    )
                print(timing)
                QUERY_NUM += 1

        results.append(
            Result.create_from(
                name="Queries", results=query_results, stopwatch=stop_watch_
            )
        )
        res = results[-1].is_ok()

    # stop log replication
    Shell.check(
        f"./ci/jobs/scripts/functional_tests/setup_log_cluster.sh --stop-log-replication",
        verbose=True,
    )

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
