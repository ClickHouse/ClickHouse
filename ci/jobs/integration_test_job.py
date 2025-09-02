import json
import os
import re
import subprocess
import sys
import time

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
test_dir = f"{repo_dir}/ci/tmp"
MAX_FAILS_BEFORE_DROP = 5
ncpu = Utils.cpu_count()


def _get_parallel_tests_skip_list():
    skip_list_file_path = f"{repo_dir}/tests/integration/parallel_skip.json"
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
        skip_list_tests = json.load(skip_list_file)
    return sorted(list(skip_list_tests))


def _start_docker_in_docker():
    with open("./ci/tmp/docker-in-docker.log", "w") as log_file:
        dockerd_proc = subprocess.Popen(
            "./ci/jobs/scripts/docker_in_docker.sh",
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    retries = 20
    for i in range(retries):
        if Shell.check("docker info > /dev/null", verbose=True):
            break
        if i == retries - 1:
            raise RuntimeError(
                f"Docker daemon didn't responded after {retries} attempts"
            )
        time.sleep(2)
    print(f"Started docker-in-docker asynchronously with PID {dockerd_proc.pid}")


def main():
    info = Info()
    sw = Utils.Stopwatch()
    match = re.search(r"\(.*?\)", info.job_name)
    options = match.group(0)[1:-1].split(",") if match else []
    options = [to.strip() for to in options]
    only_parallel = "parallel" in options
    only_sequential = "sequential" in options

    batch_num, total_batches = 0, 0
    for to in options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        else:
            pass

    if not info.is_local_run:
        _start_docker_in_docker()

    test_files = []
    for dir_name in os.listdir("./tests/integration/"):
        if dir_name.startswith("test_"):
            test_files.extend(
                [
                    os.path.join(dir_name, file_name)
                    for file_name in os.listdir(
                        os.path.join("./tests/integration/", dir_name)
                    )
                    if file_name.endswith(".py") and file_name.startswith("test")
                ]
            )
    test_files = [
        f"{dir_name}/{file_name}".replace("./tests/integration/", "")
        for dir_name, file_name in [
            test_file.rsplit("/", 1) for test_file in test_files
        ]
    ]
    assert len(test_files) > 100
    test_files.sort()
    parallel_skip_prefixes = _get_parallel_tests_skip_list()

    # parallel_skip_prefixes sanity check
    for prefix in parallel_skip_prefixes:
        assert any(
            test_file.removeprefix("./").startswith(prefix) for test_file in test_files
        ), f"No test files found for prefix [{prefix}] in [{test_files}]"

    sequential_test_modules = [
        test_file
        for test_file in test_files
        if any(test_file.startswith(prefix) for prefix in parallel_skip_prefixes)
    ]
    parallel_test_modules = [
        test_file
        for test_file in test_files
        if test_file not in sequential_test_modules
    ]
    assert len(parallel_test_modules) > 0
    assert len(sequential_test_modules) > 0
    assert len(sequential_test_modules) + len(parallel_test_modules) == len(test_files)

    if only_sequential:
        sequential_test_modules = []
    if only_parallel:
        parallel_test_modules = []

    # Split tests on batches
    # The way we split tests on batches is not straightforward and might not be correct
    # We want to split tests so that each batch has approximately the same number of tests
    # This is a simple way to achieve this:
    # 1. Sort tests by alphabet
    # 2. Split sorted tests into batches
    # 3. For each batch, take `batch_num`th element and all elements after it
    workers = max(ncpu // 4, 1)
    if batch_num > 0:
        tot_parallel_test_modules = len(parallel_test_modules)
        start_index = (batch_num - 1) * tot_parallel_test_modules // total_batches
        end_index = (batch_num) * tot_parallel_test_modules // total_batches
        parallel_test_modules = parallel_test_modules[start_index:end_index]
        print(
            f"Parallel test modules for batch {batch_num}: {len(parallel_test_modules)} out of {tot_parallel_test_modules}"
        )
        print(f"ncpu: {ncpu}, number_workers: {workers}")

        tot_sequential_test_modules = len(sequential_test_modules)
        start_index = (batch_num - 1) * tot_sequential_test_modules // total_batches
        end_index = (batch_num) * tot_sequential_test_modules // total_batches
        sequential_test_modules = sequential_test_modules[start_index:end_index]
        print(
            f"Sequential test modules for batch {batch_num}: {len(sequential_test_modules)} out of {tot_sequential_test_modules}"
        )

    # Setup environment variables for tests
    test_env = {
        "CLICKHOUSE_TESTS_BASE_CONFIG_DIR": f"{Utils.cwd()}/programs/server",
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH": "/clickhouse",
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": "/clickhouse",
    }
    test_result_parallel = None
    if parallel_test_modules:
        test_result_parallel = Result.from_pytest_run(
            command=f"{' '.join(parallel_test_modules)} --report-log-exclude-logs-on-passed-tests -n {workers}",
            cwd="./tests/integration/",
            env=test_env,
        )
    test_results = test_result_parallel.results if test_result_parallel else []

    fail_num = len([r for r in [test_result_parallel] if not r.is_ok()])
    test_result_sequential = None
    if sequential_test_modules and fail_num < MAX_FAILS_BEFORE_DROP:
        test_result_sequential = Result.from_pytest_run(
            command=f"{' '.join(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests",
            cwd="./tests/integration/",
            env=test_env,
        )
    test_results += test_result_sequential.results if test_result_sequential else []

    Result.create_from(results=test_results, stopwatch=sw).complete_job()


if __name__ == "__main__":
    main()
