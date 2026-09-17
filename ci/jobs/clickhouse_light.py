import argparse
import atexit
import dataclasses
import fcntl
import http.client
import os
import pty
import re
import select
import shlex
import subprocess
import sys
import time
import urllib.parse
from enum import Enum
from pathlib import Path

sys.path.append("./")
from typing import List

from ci.jobs.scripts.stack_trace_reader import StackTraceReader
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell


@dataclasses.dataclass
class QueryResultDescription:
    expected_results: List = dataclasses.field(default_factory=list)
    actual_results: List = dataclasses.field(default_factory=list)
    exception: str = ""


@dataclasses.dataclass
class QueryResult:
    query: str
    status: str
    description: QueryResultDescription = dataclasses.field(
        default_factory=QueryResultDescription
    )


@dataclasses.dataclass
class TestResult:
    test_name: str
    status: str
    query_results: List[QueryResult] = dataclasses.field(default_factory=list)
    tags: List[str] = dataclasses.field(default_factory=list)

    def to_praktika_result(self):
        info = ""
        for idx, query_result in enumerate(self.query_results):
            if query_result.status != Result.StatusExtended.OK:
                info += f"Query ({idx+1}):\n"
                info += f"{query_result.query}\n\n"
                if query_result.description.exception:
                    errors_before_trace, trace, errors_after_trace, unknown_lines = (
                        StackTraceReader.get_trace_and_errors(
                            stderr=query_result.description.exception
                        )
                    )
                    if errors_before_trace:
                        for idx, error in enumerate(errors_before_trace):
                            info += f"Error {idx+1}:\n"
                            info += f"{error}\n\n"
                        info += "---\n\n"
                    if trace:
                        info += "Stack trace:\n"
                        info += f"{trace}\n\n"
                        info += "---\n\n"
                    if errors_after_trace:
                        for idx, error in enumerate(errors_after_trace):
                            info += f"Error {idx+1}:\n"
                            info += f"{error}\n\n"
                        info += "---\n\n"
                    if unknown_lines:
                        info += "Unknown lines:\n"
                        for line in unknown_lines:
                            info += f"{line}\n"
                        info += "---\n\n"
                elif (
                    query_result.description.expected_results
                    != query_result.description.actual_results
                ):
                    info += f"Result mismatch:\n"
                    info += f"Expected: {query_result.description.expected_results}\n"
                    info += f"Actual:   {query_result.description.actual_results}\n"
                else:
                    assert False, "BUG"

        r = Result(
            name=self.test_name,
            status=self.status,
            info=info,
        )
        if "xfail" in self.tags:
            r.set_label("xfail")
        return r


class ClickHouseSetup:
    def __init__(self, work_dir, binary_path=""):
        self.work_dir = Path(work_dir)
        self.config_file = self.work_dir / "config.xml"
        self.server_log = self.work_dir / "clickhouse-server.log"
        self.server_err_log = self.work_dir / "clickhouse-server.err.log"
        self.client_log = self.work_dir / "client.log"
        self.status_file = self.work_dir / "status"
        if not binary_path:
            self.binary_path = self.work_dir / "clickhouse"
        else:
            self.binary_path = binary_path

        if not self.work_dir.exists():
            print(f"Work directory {self.work_dir} does not exist. Creating it.")
            self.work_dir.mkdir(parents=True, exist_ok=True)

        if self.status_file.exists():
            self.status_file.unlink()

        self.proc = None
        self.start_command = f"{self.binary_path} server -- --path {self.work_dir} --user_files_path {self.work_dir}/user_files --logger.log {self.server_log} --logger.errorlog {self.server_err_log} > /dev/null 2>&1"

    def start_server(self):
        if not self.proc or self.proc.poll() is not None:
            self.proc = subprocess.Popen(
                self.start_command,
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE,
                shell=True,
                start_new_session=True,
            )

        time.sleep(2)
        return self

    def send_query(self, query, timeout=1):
        query = query.strip()
        return Shell.get_res_stdout_stderr(
            f"{self.binary_path} client --query {shlex.quote(query)} --max_execution_time {timeout} --throw_if_no_data_to_insert=1"
        )

    def send_http(self, query, timeout=10):
        client = http.client.HTTPConnection(
            host="localhost", port=8123, timeout=timeout
        )

        timeout = int(timeout)
        params = {
            "query": query,
            "database": "system",
            # "connect_timeout": timeout,
            # "receive_timeout": timeout,
            # "send_timeout": timeout,
            # "http_connection_timeout": timeout,
            # "http_receive_timeout": timeout,
            # "http_send_timeout": timeout,
            "output_format_parallel_formatting": 0,
            "max_rows_to_read": 0,
            "stacktrace": 1,
            "http_write_exception_in_output_format": 1,
        }

        try:
            client.request(
                "POST",
                f"/?{urllib.parse.urlencode(params)}",
            )
            res = client.getresponse()
            data = res.read()

            if res.status != 200:
                return res.status, "", data.decode()

            return 0, data.decode(), ""
        except http.client.RemoteDisconnected as e:
            return -1, "", f"Connection error: Remote end closed connection - {str(e)}"
        except ConnectionRefusedError as e:
            return -1, "", f"Connection error: Connection refused - {str(e)}"
        except TimeoutError as e:
            return -1, "", f"Connection error: Timeout - {str(e)}"
        except Exception as e:
            return -1, "", f"Connection error: {type(e).__name__} - {str(e)}"
        finally:
            client.close()

    def open_client_interactive(self):
        return subprocess.Popen(
            f"{self.binary_path} client",
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            shell=True,
            start_new_session=True,
        )

    def run_test(self, test_name):
        test_name = test_name.split(".")[0]
        test_file = Path(f"./ci/jobs/queries/{test_name}.sql")
        expected_results_file = Path(f"./ci/jobs/queries/{test_name}.reference")
        result = {}
        if not test_file.exists():
            raise Exception(f"Test file {test_file} does not exist")

        with open(test_file, "r") as f:
            queries = f.readlines()

        if expected_results_file.exists():
            with open(expected_results_file, "r") as f:
                expected_results = [line.rstrip("\n") for line in f.readlines()]
        else:
            expected_results = []

        # Parse tags from comments
        tags = set()
        for query in queries:
            query_stripped = query.strip()
            if query_stripped.startswith("-- Tags:"):
                tag_line = query_stripped[len("-- Tags:") :].strip()
                tags.update(tag.strip() for tag in tag_line.split(","))

        test_result = TestResult(test_name, status=Result.StatusExtended.OK)

        for query in queries:
            query = query.strip()
            if query == "" or query.startswith("--"):
                continue

            query_result = QueryResult(query, status=Result.StatusExtended.FAIL)

            status_code, stdout, stderr = self.send_query(query)

            if stderr:
                query_result.description.exception = stderr
            else:
                result_lines = stdout.split("\n") if stdout else []
                expected_lines = expected_results[: len(result_lines)]
                if result_lines != expected_lines:
                    query_result.description.expected_results = expected_lines
                    query_result.description.actual_results = result_lines
                else:
                    query_result.status = Result.StatusExtended.OK

            test_result.query_results.append(query_result)
            if query_result.status != Result.StatusExtended.OK:
                test_result.status = Result.StatusExtended.FAIL

        # Apply xfail logic: invert status if xfail tag is present
        if "xfail" in tags:
            if "xfail" not in test_result.tags:
                test_result.tags.append("xfail")
            if test_result.status == Result.StatusExtended.FAIL:
                test_result.status = Result.StatusExtended.OK
            elif test_result.status == Result.StatusExtended.OK:
                test_result.status = Result.StatusExtended.FAIL

        return test_result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Starts ClickHouse server in the specified work directory."
    )
    parser.add_argument("--workdir", help="Work directory", default="./ci/tmp/wd")
    parser.add_argument("--path", help="Path to the clickhouse binary", default="")
    parser.add_argument(
        "--test",
        help="Optional. Space-separated test name patterns",
        default=[],
        nargs="+",
        action="extend",
    )
    args = parser.parse_args()
    Shell.check(f"rm -rf /home/max/work/ClickHouse/ci/tmp/wd/")
    Shell.check(f"chmod +x {args.path}")
    Shell.check(f"{args.path} --version", strict=True)

    def cleanup():
        Shell.check(f"pkill -f 'clickhouse server'", verbose=True, strict=False)

    atexit.register(cleanup)
    CH = ClickHouseSetup(args.workdir, args.path)

    results = []
    tests = [f for f in os.listdir("./ci/jobs/queries") if f.endswith(".sql")]
    if args.test:
        tests = [
            test for test in tests if any(pattern in test for pattern in args.test)
        ]
    assert tests
    for test_name in tests:
        CH.start_server()
        results.append(CH.run_test(test_name).to_praktika_result())

    Result.create_from(name=Info().job_name, results=results).complete_job()
