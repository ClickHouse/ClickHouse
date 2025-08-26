import csv
import glob
import heapq
import json
import logging
import os
import random
import re
import shlex
import shutil
import signal
import subprocess
import time
from collections import OrderedDict, defaultdict
from itertools import chain
from statistics import median
from typing import Any, Dict, Final, List, Optional, Set, Tuple

import requests
import yaml  # type: ignore[import-untyped]
from tests.integration.integration_test_images import IMAGES

from ci.praktika.info import Info
from ci.praktika.utils import Shell

CLICKHOUSE_PLAY_HOST = os.environ.get("CLICKHOUSE_PLAY_HOST", "play.clickhouse.com")
CLICKHOUSE_PLAY_USER = os.environ.get("CLICKHOUSE_PLAY_USER", "play")
CLICKHOUSE_PLAY_PASSWORD = os.environ.get("CLICKHOUSE_PLAY_PASSWORD", "")
CLICKHOUSE_PLAY_DB = os.environ.get("CLICKHOUSE_PLAY_DB", "default")
CLICKHOUSE_PLAY_URL = f"https://{CLICKHOUSE_PLAY_HOST}/"

MAX_RETRY = 1
NUM_WORKERS = 4
SLEEP_BETWEEN_RETRIES = 5
PARALLEL_GROUP_SIZE = 100
CLICKHOUSE_BINARY_PATH = "usr/bin/clickhouse"

FLAKY_TRIES_COUNT = 2  # run whole pytest several times
FLAKY_REPEAT_COUNT = 3  # runs test case in single module several times
MAX_TIME_SECONDS = 3600

MAX_TIME_IN_SANDBOX = 20 * 60  # 20 minutes
TASK_TIMEOUT = 8 * 60 * 60  # 8 hours

NO_CHANGES_MSG = "Nothing to run"

JOB_TIMEOUT_TEST_NAME = "Job Timeout Expired"


# Search test by the common prefix.
# This is accept tests w/o parameters in skip list.
#
# Examples:
# - has_test(['foobar'], 'foobar[param]') == True
# - has_test(['foobar[param]'], 'foobar') == True
def has_test(tests: List[str], test_to_match: str) -> bool:
    for test in tests:
        if len(test_to_match) < len(test):
            if test.startswith(test_to_match):
                return True
        else:
            if test_to_match.startswith(test):
                return True
    return False


def get_changed_tests_to_run(changed_files, repo_path):
    result = set()

    if changed_files is None:
        return []

    for fpath in changed_files:
        if re.search(r"tests/integration/test_.*/test.*\.py", fpath) is not None:
            logging.info("File %s changed and seems like integration test", fpath)
            result.add("/".join(fpath.split("/")[2:]))
    return filter_existing_tests(result, repo_path)


def filter_existing_tests(tests_to_run, repo_path):
    result = []
    for relative_test_path in tests_to_run:
        if os.path.exists(
            os.path.join(repo_path, "tests/integration", relative_test_path)
        ):
            result.append(relative_test_path)
        else:
            logging.info(
                "Skipping test %s, seems like it was removed", relative_test_path
            )
    return result


def _get_deselect_option(tests):
    return " ".join([f"--deselect {t}" for t in tests])


# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def clear_ip_tables_and_restart_daemons():
    logging.info(
        "Dump iptables after run %s",
        subprocess.check_output("sudo iptables -nvL", shell=True),
    )
    try:
        logging.info("Killing all alive docker containers")
        subprocess.check_output(
            "timeout --verbose --signal=KILL 10m docker ps --quiet | xargs --no-run-if-empty docker kill",
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        logging.info("docker kill excepted: %s", str(err))

    try:
        logging.info("Removing all docker containers")
        subprocess.check_output(
            "timeout --verbose --signal=KILL 10m docker ps --all --quiet | xargs --no-run-if-empty docker rm --force",
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        logging.info("docker rm excepted: %s", str(err))

    # don't restart docker if it's disabled
    if os.environ.get("CLICKHOUSE_TESTS_RUNNER_RESTART_DOCKER", "1") == "1":
        try:
            logging.info("Stopping docker daemon")
            subprocess.check_output("service docker stop", shell=True)
        except subprocess.CalledProcessError as err:
            logging.info("docker stop excepted: %s", str(err))

        try:
            for i in range(200):
                try:
                    logging.info("Restarting docker %s", i)
                    subprocess.check_output("service docker start", shell=True)
                    subprocess.check_output("docker ps", shell=True)
                    break
                except subprocess.CalledProcessError as err:
                    time.sleep(0.5)
                    logging.info("Waiting docker to start, current %s", str(err))
            else:
                raise RuntimeError("Docker daemon doesn't responding")
        except subprocess.CalledProcessError as err:
            logging.info("Can't reload docker: %s", str(err))

    iptables_iter = 0
    try:
        for i in range(1000):
            iptables_iter = i
            # when rules will be empty, it will raise exception
            subprocess.check_output("sudo iptables -D DOCKER-USER 1", shell=True)
    except subprocess.CalledProcessError as err:
        logging.info(
            "All iptables rules cleared, %s iterations, last error: %s",
            iptables_iter,
            str(err),
        )


class ClickhouseIntegrationTestsRunner:
    def __init__(self, repo_path: str, result_path: str, params: dict):
        self.repo_path = repo_path
        self.result_path = result_path
        self.params = params

        self.image_versions = self.params["docker_images_with_versions"]
        self.shuffle_groups = self.params["shuffle_test_groups"]
        self.flaky_check = "flaky check" in self.params["context_name"]
        self.bugfix_validate_check = "bugfix" in self.params["context_name"].lower()
        # if use_tmpfs is not set we assume it to be true, otherwise check
        self.use_tmpfs = "use_tmpfs" not in self.params or self.params["use_tmpfs"]
        self.disable_net_host = (
            "disable_net_host" in self.params and self.params["disable_net_host"]
        )
        self.start_time = time.time()
        self.soft_deadline_time = self.start_time + (TASK_TIMEOUT - MAX_TIME_IN_SANDBOX)

        self.use_old_analyzer = (
            os.environ.get("CLICKHOUSE_USE_OLD_ANALYZER") is not None
        )

        self.use_distributed_plan = (
            os.environ.get("CLICKHOUSE_USE_DISTRIBUTED_PLAN") is not None
        )

        if "run_by_hash_total" in self.params:
            self.run_by_hash_total = self.params["run_by_hash_total"]
            self.run_by_hash_num = self.params["run_by_hash_num"]
        else:
            self.run_by_hash_total = 0
            self.run_by_hash_num = 0

        # release/asan/tsan/msan
        self.job_configuration = self.params.get("job_configuration", "")
        self.pr_updated_at = self.params.get("pr_updated_at", "")

        self._all_tests = []  # type: List[str]
        self._tests_by_hash = []  # type: List[str]

    def path(self):
        return self.result_path

    def base_path(self):
        return os.path.join(str(self.result_path), "../")

    @staticmethod
    def should_skip_tests():
        return []

    def get_image_with_version(self, name):
        if name in self.image_versions:
            return name + ":" + self.image_versions[name]
        logging.warning(
            "Cannot find image %s in params list %s", name, self.image_versions
        )
        if ":" not in name:
            return name + ":latest"
        return name

    def get_image_version(self, name: str) -> Any:
        if name in self.image_versions:
            return self.image_versions[name]
        logging.warning(
            "Cannot find image %s in params list %s", name, self.image_versions
        )
        return "latest"

    def shuffle_test_groups(self):
        return self.shuffle_groups != 0

    def _pre_pull_images(self):
        image_cmd = self._get_runner_image_cmd()

        cmd = f"{self.repo_path}/tests/integration/runner {self._get_runner_opts()} {image_cmd} --pre-pull --command ' echo Pre Pull finished ' "
        Shell.check(cmd, retries=3, verbose=True, strict=True)

    @staticmethod
    def _parse_report(
        report_path: str,
    ) -> Tuple[Dict[str, Set[str]], Dict[str, float]]:
        def worst_status(current: Optional[str], new: str) -> str:
            new = new.upper()  # report["outcome"] is in lower case
            if current is None:
                return new
            for status in statuses:
                if status in (current, new):
                    return status
            raise ValueError(
                f"The previous `{current}` and new `{new}` statuses are unexpected"
            )

        tests_results = {}  # type: Dict[str,str]
        statuses = ["ERROR", "FAILED", "SKIPPED", "PASSED"]  # type: Final
        counters = {key: set() for key in statuses}  # type: Dict[str, Set[str]]
        times = {}  # type: Dict[str, float]
        with open(report_path, "r", encoding="utf-8") as rfd:
            reports = [json.loads(l) for l in rfd]
        for report in reports:
            if report["$report_type"] != "TestReport":
                continue
            # Report file contains a few reports for same test: setup, call, teardown
            test_name = report["nodeid"]

            # Parse test result status
            tests_results[test_name] = worst_status(
                tests_results.get(test_name), report["outcome"]
            )
            # Parse test times
            times[test_name] = times.get(test_name, 0) + report["duration"]

        for test, result in tests_results.items():
            counters[result].add(test)
        return (counters, times)

    @staticmethod
    def _can_run_with(path, opt):
        with open(path, "r", encoding="utf-8") as script:
            for line in script:
                if opt in line:
                    return True
        return False

    @staticmethod
    def _compress_logs(directory, relpaths, result_path):
        retcode = subprocess.call(
            f"sudo tar --use-compress-program='zstd --threads=0' "
            f"-cf {result_path} -C {directory} {' '.join(relpaths)}",
            shell=True,
        )
        # tar return 1 when the files are changed on compressing, we ignore it
        if retcode in (0, 1):
            return
        # but even on the fatal errors it's better to retry
        logging.error("Fatal error on compressing %s: %s", result_path, retcode)

    def _get_runner_opts(self):
        result = []
        if self.use_tmpfs:
            result.append("--tmpfs")
        if self.disable_net_host:
            result.append("--disable-net-host")
        if self.use_old_analyzer:
            result.append("--old-analyzer")
        if self.use_distributed_plan:
            result.append("--distributed-plan")

        return " ".join(result)

    @property
    def all_tests(self) -> List[str]:
        if self._all_tests:
            return self._all_tests
        image_cmd = self._get_runner_image_cmd()
        runner_opts = self._get_runner_opts()
        out_file_full = os.path.join(self.result_path, "runner_get_all_tests.log")
        report_file = "runner_get_all_tests.jsonl"
        cmd = (
            f"cd {self.repo_path}/tests/integration && PYTHONPATH='../..:.' timeout --verbose --signal=KILL 2m ./runner {runner_opts} {image_cmd} -- "
            f"--setup-plan --report-log={report_file}"
        )

        logging.info(
            "Getting all tests to the file %s with cmd: \n%s", out_file_full, cmd
        )
        with open(out_file_full, "wb") as ofd:
            try:
                subprocess.check_call(cmd, shell=True, stdout=ofd, stderr=ofd)
            except subprocess.CalledProcessError as ex:
                print("ERROR: Setting test plan failed. Output:")
                with open(out_file_full, "r", encoding="utf-8") as file:
                    for line in file:
                        print("    " + line, end="")
                raise ex

        # Add report_file to the uploaded files
        shutil.move(
            os.path.join(self.repo_path, "tests", "integration", report_file),
            os.path.join(self.result_path, report_file),
        )
        all_tests = set()
        with open(
            os.path.join(self.result_path, report_file), "r", encoding="utf-8"
        ) as rfd:
            reports = [json.loads(j) for j in rfd]

        all_tests = {
            r["nodeid"]
            for r in reports
            if r.get("when") == "setup" and r.get("outcome") == "passed"
        }

        assert all_tests

        self._all_tests = list(sorted(all_tests))
        return self._all_tests

    @staticmethod
    def _get_parallel_tests_skip_list(repo_path):
        skip_list_file_path = f"{repo_path}/tests/integration/parallel_skip.yaml"
        if (
            not os.path.isfile(skip_list_file_path)
            or os.path.getsize(skip_list_file_path) == 0
        ):
            raise ValueError(
                "There is something wrong with getting all tests list: "
                f"file '{skip_list_file_path}' is empty or does not exist."
            )

        skip_list_tests = []
        with open(skip_list_file_path, "r", encoding="utf-8") as skip_list_file:
            skip_list_tests = yaml.safe_load(skip_list_file)
        return list(sorted(skip_list_tests))

    @staticmethod
    def group_test_by_file(tests):
        result = OrderedDict()  # type: OrderedDict
        for test in tests:
            test_file = test.split("::")[0]
            if test_file not in result:
                result[test_file] = []
            result[test_file].append(test)
        return result

    @staticmethod
    def _update_counters(
        main_counters: Dict[str, List[str]], current_counters: Dict[str, Set[str]]
    ) -> None:
        for test in current_counters["PASSED"]:
            if test not in main_counters["PASSED"]:
                if test in main_counters["FAILED"]:
                    main_counters["FAILED"].remove(test)
                if test in main_counters["BROKEN"]:
                    main_counters["BROKEN"].remove(test)

                main_counters["PASSED"].append(test)

        for state in ("ERROR", "FAILED"):
            for test in current_counters[state]:
                if test in main_counters["PASSED"]:
                    main_counters["PASSED"].remove(test)
                if test not in main_counters[state]:
                    main_counters[state].append(test)

        for state in ("SKIPPED",):
            for test in current_counters[state]:
                main_counters[state].append(test)

    def _get_runner_image_cmd(self):
        image_cmd = ""
        if self._can_run_with(
            os.path.join(self.repo_path, "tests/integration", "runner"),
            "--docker-image-version",
        ):
            for img in IMAGES:
                if img == "clickhouse/integration-tests-runner":
                    runner_version = self.get_image_version(img)
                    logging.info(
                        "Can run with custom docker image version %s", runner_version
                    )
                    image_cmd += f" --docker-image-version={runner_version} "
                else:
                    if self._can_run_with(
                        os.path.join(self.repo_path, "tests/integration", "runner"),
                        "--docker-compose-images-tags",
                    ):
                        image_cmd += (
                            "--docker-compose-images-tags="
                            f"{self.get_image_with_version(img)} "
                        )
        else:
            image_cmd = ""
            logging.info("Cannot run with custom docker image version :(")
        return image_cmd

    @staticmethod
    def _find_test_data_dirs(repo_path, test_names):
        relpaths = {}
        for test_name in test_names:
            if "/" in test_name:
                test_dir = test_name[: test_name.find("/")]
            else:
                test_dir = test_name
            if os.path.isdir(os.path.join(repo_path, "tests/integration", test_dir)):
                for name in os.listdir(
                    os.path.join(repo_path, "tests/integration", test_dir)
                ):
                    relpath = os.path.join(os.path.join(test_dir, name))
                    mtime = os.path.getmtime(
                        os.path.join(repo_path, "tests/integration", relpath)
                    )
                    relpaths[relpath] = mtime
        return relpaths

    @staticmethod
    def _get_test_data_dirs_difference(new_snapshot, old_snapshot):
        res = set()
        for path in new_snapshot:
            if (path not in old_snapshot) or (old_snapshot[path] != new_snapshot[path]):
                res.add(path)
        return res

    def try_run_test_group(
        self,
        timeout,
        test_group,
        tests_in_group,
        num_tries,
        num_workers,
        repeat_count,
    ):
        try:
            return self.run_test_group(
                timeout,
                test_group,
                tests_in_group,
                num_tries,
                num_workers,
                repeat_count,
            )
        except Exception as e:
            logging.info("Failed to run %s:\n%s", test_group, e)
            counters = {
                "ERROR": [],
                "PASSED": [],
                "FAILED": [],
                "SKIPPED": [],
            }  # type: Dict
            tests_times = defaultdict(float)  # type: Dict
            for test in tests_in_group:
                counters["ERROR"].append(test)
                tests_times[test] = 0
            return counters, tests_times, []

    def run_test_group(
        self,
        timeout,
        test_group,
        tests_in_group,
        num_tries,
        num_workers,
        repeat_count,
    ):
        counters = {
            "ERROR": [],
            "PASSED": [],
            "FAILED": [],
            "SKIPPED": [],
            "BROKEN": [],
            "NOT_FAILED": [],
        }  # type: Dict
        tests_times = defaultdict(float)  # type: Dict

        if self.soft_deadline_time < time.time():
            for test in tests_in_group:
                logging.info("Task timeout exceeded, skipping %s", test)
                counters["SKIPPED"].append(test)
                tests_times[test] = 0
            return counters, tests_times, []

        image_cmd = self._get_runner_image_cmd()
        test_group_str = test_group.replace("/", "_").replace(".", "_")

        log_paths = []
        test_data_dirs = {}

        for i in range(num_tries):
            if timeout_expired:
                print("Timeout expired - break test group execution")
                break
            logging.info("Running test group %s for the %s retry", test_group, i)
            clear_ip_tables_and_restart_daemons()

            test_names = set([])
            for test_name in tests_in_group:
                if test_name not in counters["PASSED"]:
                    test_names.add(test_name)

            if i == 0:
                test_data_dirs = self._find_test_data_dirs(self.repo_path, test_names)

            report_name = f"{test_group_str}_{i}.jsonl"
            report_path = os.path.join(self.repo_path, "tests/integration", report_name)

            test_cmd = " ".join([shlex.quote(test) for test in sorted(test_names)])
            parallel_cmd = f" --parallel {num_workers} " if num_workers > 0 else ""
            # Run flaky tests in a random order to increase chance to catch an error
            repeat_cmd = (
                f" --count {repeat_count} --random-order " if repeat_count > 0 else ""
            )
            # -r -- show extra test summary:
            # -f -- (f)ailed
            # -E -- (E)rror
            # -p -- (p)assed
            # -s -- (s)kipped
            cmd = (
                f"cd {self.repo_path}/tests/integration && "
                f"PYTHONPATH=../..:. timeout --verbose --signal=KILL {timeout} ./runner {self._get_runner_opts()} "
                f"{image_cmd} -t {test_cmd} {parallel_cmd} {repeat_cmd} -- "
                f"-rfEps --run-id={i} --color=no --durations=0 "
                f"--report-log={report_name} --report-log-exclude-logs-on-passed-tests "
                f"{_get_deselect_option(self.should_skip_tests())}"
            )

            log_basename = f"{test_group_str}_{i}.log"
            log_path = os.path.join(self.repo_path, "tests/integration", log_basename)
            logging.info("Executing cmd: %s", cmd)
            _ret_code = Shell.run(command=cmd, log_file=log_path)

            extra_logs_names = [log_basename]
            log_result_path = os.path.join(
                self.path(), "integration_run_" + log_basename
            )
            shutil.copy(log_path, log_result_path)
            log_paths.append(log_result_path)

            for pytest_log_path in glob.glob(
                os.path.join(self.repo_path, "tests/integration/pytest*.log")
            ):
                new_name = f"{test_group_str}_{i}_{os.path.basename(pytest_log_path)}"
                os.rename(
                    pytest_log_path,
                    os.path.join(self.repo_path, "tests/integration", new_name),
                )
                extra_logs_names.append(new_name)

            dockerd_log_path = os.path.join(
                self.repo_path, "tests/integration/dockerd.log"
            )
            if os.path.exists(dockerd_log_path):
                new_name = f"{test_group_str}_{i}_{os.path.basename(dockerd_log_path)}"
                os.rename(
                    dockerd_log_path,
                    os.path.join(self.repo_path, "tests/integration", new_name),
                )
                extra_logs_names.append(new_name)

            if os.path.exists(report_path):
                extra_logs_names.append(report_name)
                new_counters, new_tests_times = self._parse_report(report_path)
                for state, tests in new_counters.items():
                    logging.info(
                        "Tests with %s state (%s): %s", state, len(tests), tests
                    )
                self._update_counters(counters, new_counters)
                for test_name, test_time in new_tests_times.items():
                    tests_times[test_name] = test_time

            test_data_dirs_new = self._find_test_data_dirs(self.repo_path, test_names)
            test_data_dirs_diff = self._get_test_data_dirs_difference(
                test_data_dirs_new, test_data_dirs
            )
            test_data_dirs = test_data_dirs_new

            if extra_logs_names or test_data_dirs_diff:
                extras_result_path = os.path.join(
                    self.path(), f"integration_run_{test_group_str}_{i}.tar.zst"
                )
                self._compress_logs(
                    os.path.join(self.repo_path, "tests/integration"),
                    extra_logs_names + list(test_data_dirs_diff),
                    extras_result_path,
                )
                log_paths.append(extras_result_path)

            if len(counters["PASSED"]) == len(tests_in_group):
                logging.info("All tests from group %s passed", test_group)
                break
            if (
                len(counters["PASSED"]) >= 0
                and len(counters["FAILED"]) == 0
                and len(counters["ERROR"]) == 0
            ):
                logging.info(
                    "Seems like all tests passed but some of them are skipped or "
                    "deselected. Ignoring them and finishing group."
                )
                break
        else:
            # Mark all non tried tests as errors, with '::' in name
            # (example test_partition/test.py::test_partition_simple). For flaky check
            # we run whole test dirs like "test_odbc_interaction" and don't
            # want to mark them as error so we filter by '::'.
            for test in tests_in_group:
                if (
                    test
                    not in chain(
                        counters["PASSED"],
                        counters["ERROR"],
                        counters["SKIPPED"],
                        counters["FAILED"],
                        counters["BROKEN"],
                    )
                    and "::" in test
                ):
                    counters["ERROR"].append(test)

        return counters, tests_times, log_paths

    def run_flaky_check(self, should_fail=False):
        tests_to_run = get_changed_tests_to_run(
            self.params["changed_files"], self.repo_path
        )
        if not tests_to_run:
            logging.info("No integration tests to run found")
            return "success", NO_CHANGES_MSG, [(NO_CHANGES_MSG, "OK")], ""

        logging.info("Found '%s' tests to run", " ".join(tests_to_run))
        result_state = "success"
        description_prefix = "No failed tests: "
        logging.info("Starting check with retries")
        final_retry = 0
        counters = {
            "ERROR": [],
            "PASSED": [],
            "FAILED": [],
            "SKIPPED": [],
            "BROKEN": [],
            "NOT_FAILED": [],
        }  # type: Dict
        tests_times = defaultdict(float)  # type: Dict
        tests_log_paths = defaultdict(list)
        id_counter = 0
        for test_to_run in tests_to_run:
            tries_num = 1 if should_fail else FLAKY_TRIES_COUNT
            for i in range(tries_num):
                if timeout_expired:
                    print("Timeout expired - break flaky check execution")
                    break
                final_retry += 1
                logging.info("Running tests for the %s time", i)
                group_counters, group_test_times, log_paths = self.try_run_test_group(
                    "3h",
                    f"bugfix_{id_counter}" if should_fail else f"flaky{id_counter}",
                    [test_to_run],
                    1,
                    1,
                    FLAKY_REPEAT_COUNT,
                )
                id_counter = id_counter + 1
                for counter, value in group_counters.items():
                    logging.info(
                        "Tests from group %s stats, %s count %s",
                        test_to_run,
                        counter,
                        len(value),
                    )
                    counters[counter] += value
                    for test_name in value:
                        tests_log_paths[test_name] = log_paths

                for test_name, test_time in group_test_times.items():
                    tests_times[test_name] = test_time
                if not should_fail and (
                    group_counters["FAILED"] or group_counters["ERROR"]
                ):
                    logging.info(
                        "Unexpected failure in group %s. Fail fast for current group",
                        test_to_run,
                    )
                    break

        if counters["FAILED"]:
            logging.info("Found failed tests: %s", " ".join(counters["FAILED"]))
            description_prefix = "Failed tests found: "
            result_state = "failure"
        if counters["ERROR"]:
            description_prefix = "Failed tests found: "
            logging.info("Found error tests: %s", " ".join(counters["ERROR"]))
            # NOTE "error" result state will restart the whole test task,
            # so we use "failure" here
            result_state = "failure"
        logging.info("Try is OK, all tests passed, going to clear env")
        clear_ip_tables_and_restart_daemons()
        logging.info("And going to sleep for some time")
        time.sleep(5)

        test_result = []
        for state in ("ERROR", "FAILED", "PASSED", "SKIPPED"):
            if state == "PASSED":
                text_state = "OK"
            elif state == "FAILED":
                text_state = "FAIL"
            else:
                text_state = state
            test_result += [
                (c, text_state, f"{tests_times[c]:.2f}", tests_log_paths[c])
                for c in counters[state]
            ]

        status_text = description_prefix + ", ".join(
            [
                str(n).lower().replace("failed", "fail") + ": " + str(len(c))
                for n, c in counters.items()
            ]
        )

        return result_state, status_text, test_result, tests_log_paths

    def run_impl(self):
        if self.flaky_check or self.bugfix_validate_check:
            result_state, status_text, test_result, tests_log_paths = (
                self.run_flaky_check(should_fail=self.bugfix_validate_check)
            )
        else:
            result_state, status_text, test_result, tests_log_paths = (
                self.run_normal_check()
            )

        if self.soft_deadline_time < time.time():
            status_text = "Timeout, " + status_text
            result_state = "failure"

        if timeout_expired:
            logging.error(
                "Job killed by external timeout signal - setting status to failure!"
            )
            status_text = "Job timeout expired, " + status_text
            result_state = "failure"
            # add mock test case to make timeout visible in job report and in ci db
            test_result.insert(0, (JOB_TIMEOUT_TEST_NAME, "FAIL", "", ""))

        if "(memory)" in self.params["context_name"]:
            result_state = "success"

        return result_state, status_text, test_result, tests_log_paths

    def get_tests_execution_time(self):
        start_time_filter = "toStartOfDay(now())"
        if self.pr_updated_at:
            start_time_filter = f"parseDateTimeBestEffort('{self.pr_updated_at}')"

        query = f"""
            SELECT
                file,
                round(sum(test_duration_ms)) AS file_duration_ms
            FROM
            (
                SELECT
                    splitByString('::', test_name)[1] AS file,
                    median(test_duration_ms) AS test_duration_ms
                FROM checks
                WHERE (check_name LIKE 'Integration%')
                    AND (check_start_time >= ({start_time_filter} - toIntervalDay(30)))
                    AND (check_start_time <= ({start_time_filter} - toIntervalHour(2)))
                    AND ((head_ref = 'master') AND startsWith(head_repo, 'ClickHouse/'))
                    AND (test_name != '')
                    AND (test_status != 'SKIPPED')
                GROUP BY test_name
            )
            GROUP BY file
            ORDER BY ALL
            FORMAT JSON
        """
        logging.info(query)

        url = (
            f"{CLICKHOUSE_PLAY_URL}/?user={CLICKHOUSE_PLAY_USER}"
            f"&password={requests.compat.quote(CLICKHOUSE_PLAY_PASSWORD)}"
            f"&query={requests.compat.quote(query)}"
        )
        max_retries = 3
        retry_delay_seconds = 5

        for attempt in range(max_retries):
            try:
                logging.info(
                    "Querying play via HTTP (Attempt %s/%s)...",
                    attempt + 1,
                    max_retries,
                )

                response = requests.get(url, timeout=120)
                response.raise_for_status()
                result_data = response.json().get("data", [])
                tests_execution_times = {
                    row["file"]: float(row["file_duration_ms"]) for row in result_data
                }

                logging.info(
                    "Successfully fetched execution times for %s modules via HTTP.",
                    len(tests_execution_times),
                )
                return tests_execution_times

            except requests.exceptions.RequestException as e:
                logging.warning(
                    "Attempt %s/%s failed to fetch test times via HTTP: %s",
                    attempt + 1,
                    max_retries,
                    e,
                )
                if attempt < max_retries - 1:
                    logging.info("Retrying in %s seconds...", retry_delay_seconds)
                    time.sleep(retry_delay_seconds)
                else:
                    logging.error(
                        "All %s attempts failed. Could not fetch test times.",
                        max_retries,
                    )
                    raise

    def group_tests_by_execution_time(self) -> List[List[str]]:
        """
        Groups tests into balanced deterministic sets using historical execution time
        """
        num_groups = self.run_by_hash_total
        logging.info("Splitting tests by execution time into %s groups", num_groups)
        if num_groups <= 0:
            return []

        file_to_test_map = self.group_test_by_file(self.all_tests)
        all_current_files = sorted(list(file_to_test_map.keys()))

        sequential_test_prefixes = [
            p.split("::", 1)[0]
            for p in self._get_parallel_tests_skip_list(self.repo_path)
        ]
        sequential_files_list = []
        parallel_files_list = []

        for file in all_current_files:
            is_sequential = any(
                file.startswith(prefix) for prefix in sequential_test_prefixes
            )
            if is_sequential:
                sequential_files_list.append(file)
            else:
                parallel_files_list.append(file)

        tests_execution_times = self.get_tests_execution_time()
        assert (
            len(tests_execution_times) > 400
        ), f"Number of tests should be more than 400. Actual {len(tests_execution_times)}"
        known_db_modules_set = set(tests_execution_times.keys())

        parallel_tests_with_known_time: Dict[str, float] = {
            mod: tests_execution_times[mod]
            for mod in parallel_files_list
            if mod in known_db_modules_set
        }
        new_parallel_tests: List[str] = [
            mod for mod in parallel_files_list if mod not in known_db_modules_set
        ]
        sequential_tests_with_known_time: Dict[str, float] = {
            mod: tests_execution_times[mod]
            for mod in sequential_files_list
            if mod in known_db_modules_set
        }
        new_sequential_tests: List[str] = [
            mod for mod in sequential_files_list if mod not in known_db_modules_set
        ]

        logging.info(
            "Found %s parallel modules with known execution time.",
            len(parallel_tests_with_known_time),
        )
        logging.info("Found %s new parallel modules:", len(new_parallel_tests))
        logging.info("%s", str(new_parallel_tests))
        logging.info(
            "Found %s sequential modules with known execution time.",
            len(sequential_tests_with_known_time),
        )
        logging.info("Found %s new sequential modules:", len(new_sequential_tests))
        logging.info("%s", str(new_sequential_tests))

        # balance parallel tests
        parallel_heap = [(0, i, []) for i in range(num_groups)]
        heapq.heapify(parallel_heap)

        sorted_timed_parallel = sorted(
            parallel_tests_with_known_time.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        for file, time in sorted_timed_parallel:
            total_time, group_index, files = heapq.heappop(parallel_heap)
            files.append(file)
            heapq.heappush(parallel_heap, (total_time + time, group_index, files))

        for file in new_parallel_tests:
            total_time, group_index, files = heapq.heappop(parallel_heap)
            files.append(file)
            heapq.heappush(parallel_heap, (total_time, group_index, files))

        test_file_groups = [[] for _ in range(num_groups)]
        parallel_group_times = [0.0] * num_groups
        while parallel_heap:
            total_time, group_index, files = heapq.heappop(parallel_heap)
            test_file_groups[group_index] = files
            parallel_group_times[group_index] = total_time

        # balance sequential tests using a separate heap
        # if using single heap, it will not be distributed equally
        sequential_heap = [(parallel_group_times[i], i) for i in range(num_groups)]
        heapq.heapify(sequential_heap)

        sorted_timed_sequential = sorted(
            sequential_tests_with_known_time.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        for file, time in sorted_timed_sequential:
            total_time, group_index = heapq.heappop(sequential_heap)
            test_file_groups[group_index].append(file)
            heapq.heappush(sequential_heap, (total_time + time, group_index))

        for file in new_sequential_tests:
            total_time, group_index = heapq.heappop(sequential_heap)
            test_file_groups[group_index].append(file)
            heapq.heappush(sequential_heap, (total_time, group_index))

        # heap contains final total times
        total_group_times = [0.0] * num_groups
        while sequential_heap:
            total_time, group_index = heapq.heappop(sequential_heap)
            total_group_times[group_index] = total_time

        # sort tests in groups by execution time to run slow tests first
        def module_sort_key(m):
            return (-tests_execution_times.get(m, -1.0), m)

        for i in range(len(test_file_groups)):
            test_file_groups[i].sort(key=module_sort_key)

        # expand groups
        final_test_groups = []
        for group_of_test_files in test_file_groups:
            test_name_group = []
            for module_name in group_of_test_files:
                tests_in_file = sorted(file_to_test_map.get(module_name, []))
                test_name_group.extend(tests_in_file)
            final_test_groups.append(test_name_group)

        # log
        for i, test_group in enumerate(final_test_groups):
            total_time = total_group_times[i]
            parallel_time = parallel_group_times[i]
            sequential_time = total_time - parallel_time
            logging.info(
                "Group %d | Tests: %d | Total time: %.2fs (parallel: %.2fs, sequential: %.2fs)",
                i,
                len(test_group),
                total_time,
                parallel_time,
                sequential_time,
            )

        return final_test_groups

    @property
    def tests_by_hash(self) -> List[str]:
        "Tries it's best to group the tests equally between groups"
        if self._tests_by_hash:
            return self._tests_by_hash
        if self.run_by_hash_total == 0:
            self._tests_by_hash = self.all_tests
            return self._tests_by_hash

        # Split tests in groups by historical execution time
        try:
            all_groups = self.group_tests_by_execution_time()
            self._tests_by_hash = all_groups[self.run_by_hash_num]
            return self._tests_by_hash
        except Exception as e:
            logging.error("Can't split tests by execution time: %s", e)
            logging.error(e)

        # Fallback in case play server doesn't work
        # Split tests in groups by number of tests in group
        grouped_tests = self.group_test_by_file(self.all_tests)
        groups_by_hash = {
            g: [] for g in range(self.run_by_hash_total)
        }  # type: Dict[int, List[str]]
        for tests_in_group in grouped_tests.values():
            # It should work determenistic, because it searches groups with min tests
            min_group = min(len(tests) for tests in groups_by_hash.values())
            # And then it takes a group with min index
            group_to_increase = min(
                g for g, t in groups_by_hash.items() if len(t) == min_group
            )
            groups_by_hash[group_to_increase].extend(tests_in_group)
        self._tests_by_hash = groups_by_hash[self.run_by_hash_num]
        return self._tests_by_hash

    def run_normal_check(self):
        logging.info("Pulling images")
        self._pre_pull_images()
        logging.info(
            "Dump iptables before run %s",
            subprocess.check_output("sudo iptables -nvL", shell=True),
        )
        parallel_skip_tests = self._get_parallel_tests_skip_list(self.repo_path)
        logging.info(
            "Found %s tests first 3 %s",
            len(self.tests_by_hash),
            " ".join(self.tests_by_hash[:3]),
        )
        # For backward compatibility, use file names in filtered_sequential_tests
        filtered_sequential_tests = list(
            set(
                test_name.split("::", maxsplit=1)[0]
                for test_name in filter(
                    lambda test: has_test(parallel_skip_tests, test), self.tests_by_hash
                )
            )
        )
        filtered_parallel_tests = list(
            filter(
                lambda test: not has_test(parallel_skip_tests, test),
                self.tests_by_hash,
            )
        )
        not_found_tests = list(
            filter(
                lambda test: not has_test(self.all_tests, test),
                parallel_skip_tests,
            )
        )
        logging.info(
            "Found %s tests first 3 %s, parallel %s, other %s",
            len(self.tests_by_hash),
            " ".join(self.tests_by_hash[:3]),
            len(filtered_parallel_tests),
            len(self.tests_by_hash) - len(filtered_parallel_tests),
        )
        logging.info(
            "Not found %s tests first 3 %s",
            len(not_found_tests),
            " ".join(not_found_tests[:3]),
        )
        grouped_tests = self.group_test_by_file(filtered_sequential_tests)
        grouped_tests["parallel"] = filtered_parallel_tests
        logging.info("Found %s tests groups", len(grouped_tests))
        counters = {
            "ERROR": [],
            "PASSED": [],
            "FAILED": [],
            "SKIPPED": [],
            "BROKEN": [],
            "NOT_FAILED": [],
        }  # type: Dict
        tests_times = defaultdict(float)
        tests_log_paths = defaultdict(list)
        items_to_run = list(grouped_tests.items())
        logging.info("Total test groups %s", len(items_to_run))
        if self.shuffle_test_groups():
            logging.info("Shuffling test groups")
            random.shuffle(items_to_run)
        for group, tests in items_to_run:
            if timeout_expired:
                print("Timeout expired - break tests execution")
                break
            logging.info("Running test group %s containing %s tests", group, len(tests))
            group_counters, group_test_times, log_paths = self.try_run_test_group(
                "2h", group, tests, MAX_RETRY, NUM_WORKERS, 0
            )
            total_tests = 0
            for counter, value in group_counters.items():
                logging.info(
                    "Tests from group %s stats, %s count %s", group, counter, len(value)
                )
                counters[counter] += value
                logging.info(
                    "Totally have %s with status %s", len(counters[counter]), counter
                )
                total_tests += len(counters[counter])
                for test_name in value:
                    tests_log_paths[test_name] = log_paths
            logging.info(
                "Totally finished tests %s/%s", total_tests, len(self.tests_by_hash)
            )

            for test_name, test_time in group_test_times.items():
                tests_times[test_name] = test_time

            if len(counters["FAILED"]) + len(counters["ERROR"]) >= 20:
                logging.info("Collected more than 20 failed/error tests, stopping")
                break
        if counters["FAILED"] or counters["ERROR"]:
            logging.info(
                "Overall status failure, because we have tests in FAILED or ERROR state"
            )
            result_state = "failure"
        else:
            logging.info("Overall success!")
            result_state = "success"
        test_result = []
        for state in (
            "ERROR",
            "FAILED",
            "PASSED",
            "SKIPPED",
            "BROKEN",
            "NOT_FAILED",
        ):
            if state == "PASSED":
                text_state = "OK"
            elif state == "FAILED":
                text_state = "FAIL"
            else:
                text_state = state
            test_result += [
                (c, text_state, f"{tests_times[c]:.2f}", tests_log_paths[c])
                for c in counters[state]
            ]
        failed_sum = len(counters["FAILED"]) + len(counters["ERROR"])
        status_text = f"fail: {failed_sum}, passed: {len(counters['PASSED'])}"

        if not counters or sum(len(counter) for counter in counters.values()) == 0:
            status_text = "No tests found for some reason! It's a bug"
            result_state = "failure"

        return result_state, status_text, test_result, tests_log_paths


def write_results(results_file, status_file, results, status):
    with open(results_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerows(results)
    with open(status_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow(status)


def run():
    signal.signal(signal.SIGTERM, handle_sigterm)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    repo_path = os.environ.get("CLICKHOUSE_TESTS_REPO_PATH", "")
    result_path = os.environ.get("CLICKHOUSE_TESTS_RESULT_PATH", "")
    params_path = os.environ.get("CLICKHOUSE_TESTS_JSON_PARAMS_PATH", "")

    assert all((repo_path, result_path, params_path))

    with open(params_path, "r", encoding="utf-8") as jfd:
        params = json.loads(jfd.read())
    runner = ClickhouseIntegrationTestsRunner(repo_path, result_path, params)

    logging.info("Running tests")

    is_ci = not Info().is_local_run
    if is_ci:
        # Avoid overlaps with previous runs
        logging.info("Clearing dmesg before run")
        subprocess.check_call("sudo -E dmesg --clear", shell=True)

    state, description, test_results, _test_log_paths = runner.run_impl()
    logging.info("Tests finished")

    if is_ci:
        # Dump dmesg (to capture possible OOMs)
        logging.info("Dumping dmesg")
        subprocess.check_call("sudo -E dmesg -T", shell=True)

    status = (state, description)
    out_results_file = os.path.join(runner.path(), "test_results.tsv")
    out_status_file = os.path.join(runner.path(), "check_status.tsv")
    write_results(out_results_file, out_status_file, test_results, status)
    logging.info("Result written")


timeout_expired = False
runner_subprocess = None  # type:Optional[TeePopen]


def handle_sigterm(signum, _frame):
    # TODO: think on how to process it without globals?
    print(f"WARNING: Received signal {signum}")
    global timeout_expired  # pylint:disable=global-statement
    timeout_expired = True


if __name__ == "__main__":
    run()
