#!/usr/bin/env python3

import os
import sys
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


# Reuse the same ClickHouseBinary helper from sqltest_job.py
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

    def start(self):
        import subprocess
        import time

        print("Starting ClickHouse server")
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
        print("ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print("ClickHouse server ready")
        else:
            print("ClickHouse server NOT ready")
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
                print("Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True


def main():
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseBinary()

    sqllogic_dir = os.path.join(Utils.cwd(), "tests", "sqllogic")
    sqllogictest_repo = os.path.join(temp_dir, "sqllogictest")
    out_dir = os.path.join(temp_dir, "sqllogic_output")
    os.makedirs(out_dir, exist_ok=True)

    # Step 1: Start ClickHouse
    print("Start ClickHouse")

    def start():
        return ch.install() and ch.start()

    results.append(
        Result.from_commands_run(
            name="Start ClickHouse",
            command=start,
        )
    )

    # Step 2: Clone sqllogictest repo
    if results[-1].is_ok():
        print("Clone sqllogictest repo")

        def clone():
            return Path(sqllogictest_repo).is_dir() or Shell.check(
                f"git clone --depth 1 https://github.com/gregrahn/sqllogictest.git {sqllogictest_repo}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Clone sqllogictest repo",
                command=clone,
            )
        )

    # Step 3: Run self-test
    if results[-1].is_ok():
        print("Run self-test")
        self_test_dir = os.path.join(sqllogic_dir, "self-test")
        self_test_out = os.path.join(out_dir, "self-test")
        os.makedirs(self_test_out, exist_ok=True)

        def self_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/self-test.log"
                f" self-test"
                f" --self-test-dir {self_test_dir}"
                f" --out-dir {self_test_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Self-test",
                command=self_test,
            )
        )

    # Step 4: Run statements-test
    if results[-1].is_ok():
        print("Run statements-test")
        statements_input = os.path.join(sqllogictest_repo, "test")
        statements_out = os.path.join(out_dir, "statements-test")
        os.makedirs(statements_out, exist_ok=True)

        def statements_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/statements-test.log"
                f" statements-test"
                f" --input-dir {statements_input}"
                f" --out-dir {statements_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Statements test",
                command=statements_test,
            )
        )

    # Step 5: Run complete-test
    if results[-1].is_ok():
        print("Run complete-test")
        complete_input = os.path.join(sqllogictest_repo, "test")
        complete_out = os.path.join(out_dir, "complete-test")
        os.makedirs(complete_out, exist_ok=True)

        def complete_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/complete-test.log"
                f" complete-test"
                f" --input-dir {complete_input}"
                f" --out-dir {complete_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Complete test",
                command=complete_test,
            )
        )

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
    ).complete_job()


if __name__ == "__main__":
    main()
