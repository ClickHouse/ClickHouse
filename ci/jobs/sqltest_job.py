#!/usr/bin/env python3

import html
import os
import random
import string
import subprocess
import time
from pathlib import Path

import yaml
from clickhouse_driver import Client
from praktika.result import Result
from praktika.utils import Shell, Utils

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
    client = Client(host="localhost", port=9000)
    log_file = open("test.log", "w")
    report_html_file = open("report.html", "w")
    summary = {"success": 0, "total": 0, "results": {}}
    database_name = "sqltest_" + "".join(
        random.choice(string.ascii_lowercase) for _ in range(10)
    )
    settings = {
        "default_table_engine": "Memory",
        "union_default_mode": "DISTINCT",
        "calculate_text_stack_trace": 0,
    }

    if True:
        print("Start ClickHouse")

        def start():
            return ch.install() and ch.start()

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=start,
            )
        )

    if results[-1].is_ok():
        print("Setup DB")

        def do():
            client.execute(
                f"DROP DATABASE IF EXISTS {database_name}", settings=settings
            )
            client.execute(f"CREATE DATABASE {database_name}", settings=settings)
            return True

        results.append(
            Result.from_commands_run(
                name="Setup DB",
                command=do,
            )
        )

    if results[-1].is_ok():
        print("Clone SQLTest repo")

        def do():
            return Path("./sqltest").is_dir() or Shell.check(
                "git clone --depth 1 https://github.com/elliotchance/sqltest.git",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Clone SQLTest repo",
                command=do,
                workdir=temp_dir,
            )
        )

    if results[-1].is_ok():
        print("Test")

        def do():
            with open("features.yml", "r") as file:
                yaml_content = yaml.safe_load(file)
            for category in yaml_content:
                log_file.write(category.capitalize() + " features:\n")
                summary["results"][category] = {"success": 0, "total": 0, "results": {}}
                for test in yaml_content[category]:
                    log_file.write(test + ": " + yaml_content[category][test] + "\n")
                    summary["results"][category]["results"][test] = {
                        "success": 0,
                        "total": 0,
                        "description": yaml_content[category][test],
                    }

                    test_path = test[0] + "/" + test + ".tests.yml"
                    if os.path.exists(test_path):
                        with open(test_path, "r") as test_file:
                            test_yaml_content = yaml.load_all(
                                test_file, Loader=yaml.FullLoader
                            )

                            for test_case in test_yaml_content:
                                queries = test_case["sql"]
                                if not isinstance(queries, list):
                                    queries = [queries]

                                for query in queries:
                                    # Example: E011-01
                                    test_group = ""
                                    if "-" in test:
                                        test_group = test.split("-", 1)[0]
                                        summary["results"][category]["results"][
                                            test_group
                                        ]["total"] += 1
                                    summary["results"][category]["results"][test][
                                        "total"
                                    ] += 1
                                    summary["results"][category]["total"] += 1
                                    summary["total"] += 1

                                    log_file.write(query + "\n")

                                    try:
                                        result = client.execute(
                                            query, settings=settings
                                        )
                                        log_file.write(str(result) + "\n")

                                        if test_group:
                                            summary["results"][category]["results"][
                                                test_group
                                            ]["success"] += 1
                                        summary["results"][category]["results"][test][
                                            "success"
                                        ] += 1
                                        summary["results"][category]["success"] += 1
                                        summary["success"] += 1

                                    except Exception as e:
                                        log_file.write(f"Error occurred: {str(e)}\n")
            return True

        client.execute(f"DROP DATABASE {database_name}", settings=settings)
        results.append(
            Result.from_commands_run(
                name="Test",
                command=do,
                workdir=f"{temp_dir}/sqltest/standards/2016/",
            )
        )
    reset_color = "</b>"

    def enable_color(ratio):
        if ratio == 0:
            return "<b style='color: red;'>"
        elif ratio < 0.5:
            return "<b style='color: orange;'>"
        elif ratio < 1:
            return "<b style='color: gray;'>"
        else:
            return "<b style='color: green;'>"

    def print_ratio(indent, name, success, total, description):
        report_html_file.write(
            "{}{}: {}{} / {} ({:.1%}){}{}\n".format(
                " " * indent,
                name.capitalize(),
                enable_color(success / total),
                success,
                total,
                success / total,
                reset_color,
                f" - " + html.escape(description) if description else "",
            )
        )

    if results[-1].is_ok():
        print("Report")

        def do():
            report_html_file.write(
                "<html><body><pre style='font-size: 16pt; padding: 1em; line-height: 1.25;'>\n"
            )
            print_ratio(0, "Total", summary["success"], summary["total"], "")
            for category in summary["results"]:
                cat_summary = summary["results"][category]

                if cat_summary["total"] == 0:
                    continue

                print_ratio(
                    2, category, cat_summary["success"], cat_summary["total"], ""
                )

                for test in summary["results"][category]["results"]:
                    test_summary = summary["results"][category]["results"][test]

                    if test_summary["total"] == 0:
                        continue

                    print_ratio(
                        6 if "-" in test else 4,
                        test,
                        test_summary["success"],
                        test_summary["total"],
                        test_summary["description"],
                    )
            report_html_file.write("</pre></body></html>\n")
            return True

        results.append(
            Result.from_commands_run(
                name="Report",
                command=do,
            )
        )
        results[-1].set_files("report.html")

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
