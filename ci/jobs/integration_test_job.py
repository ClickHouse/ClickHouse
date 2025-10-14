import argparse
import os
import subprocess
import time
from pathlib import Path

from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV, get_optimal_test_batch
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"
MAX_FAILS_BEFORE_DROP = 5
OOM_IN_DMESG_TEST_NAME = "OOM in dmesg"
ncpu = Utils.cpu_count()
mem_gb = round(Utils.physical_memory() / (1024**3), 1)
MAX_CPUS_PER_WORKER = 4
MAX_MEM_PER_WORKER = 7
MAX_WORKERS = min(ncpu // MAX_CPUS_PER_WORKER, mem_gb // MAX_MEM_PER_WORKER) or 1


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


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("--options", help="Job parameters: ...")
    parser.add_argument(
        "--test",
        help="Optional. Test name patterns (space-separated)",
        default=[],
        nargs="+",
        action="extend",
    )
    parser.add_argument(
        "--count",
        help="Optional. Number of times to repeat each test",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--debug",
        help="Optional. Open python debug console on exception",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--path",
        help="Optional. Path to custom clickhouse binary",
        type=str,
        default="",
    )
    parser.add_argument(
        "--path_1",
        help="Optional. Path to custom server config",
        type=str,
        default="",
    )
    return parser.parse_args()


FLAKY_CHECK_TEST_REPEAT_COUNT = 3
FLAKY_CHECK_MODULE_REPEAT_COUNT = 2


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    job_params = args.options.split(",") if args.options else []
    job_params = [to.strip() for to in job_params]
    use_old_analyzer = False
    use_distributed_plan = False
    is_flaky_check = False
    is_bugfix_validation = False
    is_parallel = False
    is_sequential = False
    workers = MAX_WORKERS
    java_path = Shell.get_output(
        "update-alternatives --config java | sed -n 's/.*(providing \/usr\/bin\/java): //p'",
        verbose=True,
    )
    repeat_option = ""
    if "bugfix" in info.job_name.lower():
        is_bugfix_validation = True

    batch_num, total_batches = 1, 1
    for to in job_params:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif any(build in to for build in ("amd_", "arm_")):
            build_type = to
        elif to == "old analyzer":
            use_old_analyzer = True
        elif to == "distributed plan":
            use_distributed_plan = True
        elif to == "flaky":
            is_flaky_check = True
            repeat_option = f"--count {FLAKY_CHECK_TEST_REPEAT_COUNT} --random-order"
        elif to == "parallel":
            is_parallel = True
        elif to == "sequential":
            is_sequential = True
        else:
            assert False, f"Unknown job option [{to}]"

    if args.count:
        repeat_option = f"--count {args.count}"

    changed_test_modules = []
    if is_bugfix_validation or is_flaky_check:
        changed_files = info.get_changed_files()
        for file in changed_files:
            if file.startswith("tests/integration/test") and file.endswith(".py"):
                changed_test_modules.append(file.removeprefix("tests/integration/"))

    if is_bugfix_validation:
        if Utils.is_arm():
            link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        else:
            link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        if not info.is_local_run or not (Path(temp_path) / "clickhouse").exists():
            print(
                f"NOTE: Clickhouse binary will be downloaded to [{temp_path}] from [{link_to_master_head_binary}]"
            )
            if info.is_local_run:
                time.sleep(10)
            Shell.check(
                f"wget -nv -P {temp_path} {link_to_master_head_binary}",
                verbose=True,
                strict=True,
            )

    clickhouse_path = f"{Utils.cwd()}/ci/tmp/clickhouse"
    clickhouse_server_config_dir = f"{Utils.cwd()}/programs/server"
    if info.is_local_run:
        if args.path:
            clickhouse_path = args.path
        else:
            if Path(clickhouse_path).is_file():
                pass
            elif Path(f"{Utils.cwd()}/build/programs/clickhouse").is_file():
                clickhouse_path = f"{Utils.cwd()}/build/programs/clickhouse"
            elif Path(f"{Utils.cwd()}/clickhouse").is_file():
                clickhouse_path = f"{Utils.cwd()}/clickhouse"
            else:
                raise FileNotFoundError(f"Clickhouse binary not found")
        if args.path_1:
            clickhouse_server_config_dir = args.path_1
    assert Path(
        clickhouse_server_config_dir
    ), f"Clickhouse config dir does not exist [{clickhouse_server_config_dir}]"
    print(f"Using ClickHouse binary at [{clickhouse_path}]")

    Shell.check(f"chmod +x {clickhouse_path}", verbose=True, strict=True)
    Shell.check(f"{clickhouse_path} --version", verbose=True, strict=True)

    if not Shell.check("docker info > /dev/null", verbose=True):
        _start_docker_in_docker()
    Shell.check("docker info > /dev/null", verbose=True, strict=True)

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

    parallel_test_modules, sequential_test_modules = get_optimal_test_batch(
        test_files, total_batches, batch_num, workers
    )

    if is_bugfix_validation or is_flaky_check:
        # TODO: reduce scope to modified test cases instead of entire modules
        sequential_test_modules = [
            test_file
            for test_file in changed_test_modules
            if test_file in sequential_test_modules
        ]
        parallel_test_modules = [
            test_file
            for test_file in changed_test_modules
            if test_file in parallel_test_modules
        ]

    if is_sequential:
        parallel_test_modules = []
        assert not is_parallel
    elif is_parallel:
        sequential_test_modules = []
        assert not is_sequential

    # Setup environment variables for tests
    for image_name, env_name in IMAGES_ENV.items():
        tag = info.docker_tag(image_name)
        if tag:
            print(f"Setting environment variable [{env_name}] to [{tag}]")
            os.environ[env_name] = tag
        else:
            assert False, f"No tag found for image [{image_name}]"

    test_env = {
        "CLICKHOUSE_TESTS_BASE_CONFIG_DIR": clickhouse_server_config_dir,
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_BINARY": clickhouse_path,  # some test cases support alternative binary location
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_USE_OLD_ANALYZER": "1" if use_old_analyzer else "0",
        "CLICKHOUSE_USE_DISTRIBUTED_PLAN": "1" if use_distributed_plan else "0",
        "PYTEST_CLEANUP_CONTAINERS": "1",
        "JAVA_PATH": java_path,
    }
    test_results = []
    files = []

    has_error = False
    error_info = []

    if args.test:
        test_result_specific = Result.from_pytest_run(
            command=f"{' '.join(args.test)} {'--pdb' if args.debug else ''} {repeat_option}",
            cwd="./tests/integration/",
            env=test_env,
            pytest_report_file=f"{temp_path}/pytest.jsonl",
        )
        test_results.extend(test_result_specific.results)
        if test_result_specific.files:
            files.extend(test_result_specific.files)
    else:
        module_repeat_cnt = 1 if not is_flaky_check else FLAKY_CHECK_MODULE_REPEAT_COUNT
        if parallel_test_modules:
            for attempt in range(module_repeat_cnt):
                test_result_parallel = Result.from_pytest_run(
                    command=f"{' '.join(reversed(parallel_test_modules))} --report-log-exclude-logs-on-passed-tests -n {workers} --dist=loadfile --tb=short {repeat_option}",
                    cwd="./tests/integration/",
                    env=test_env,
                    pytest_report_file=f"{temp_path}/pytest_parallel.jsonl",
                )
                if not test_result_parallel.is_ok():
                    print(
                        f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                    )
                    break
            test_results.extend(test_result_parallel.results)
            if test_result_parallel.files:
                files.extend(test_result_parallel.files)
            if test_result_parallel.is_error():
                has_error = True
                error_info.append(test_result_parallel.info)

        fail_num = len([r for r in test_results if not r.is_ok()])
        if (
            sequential_test_modules
            and fail_num < MAX_FAILS_BEFORE_DROP
            and not has_error
        ):
            for attempt in range(module_repeat_cnt):
                test_result_sequential = Result.from_pytest_run(
                    command=f"{' '.join(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests --tb=short {repeat_option} -n 1 --dist=loadfile",
                    env=test_env,
                    cwd="./tests/integration/",
                    pytest_report_file=f"{temp_path}/pytest_sequential.jsonl",
                )
                if not test_result_sequential.is_ok():
                    print(
                        f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                    )
                    break
            test_results.extend(test_result_sequential.results)
            if test_result_sequential.files:
                files.extend(test_result_sequential.files)
            if test_result_sequential.is_error():
                has_error = True
                error_info.append(test_result_sequential.info)

    # Remove iptables rule added in tests
    Shell.check("sudo iptables -D DOCKER-USER 1 ||:", verbose=True)

    if not info.is_local_run:
        print("Dumping dmesg")
        Shell.check("dmesg -T > dmesg.log", verbose=True, strict=True)
        with open("dmesg.log", "rb") as dmesg:
            dmesg = dmesg.read()
            if (
                b"Out of memory: Killed process" in dmesg
                or b"oom_reaper: reaped process" in dmesg
                or b"oom-kill:constraint=CONSTRAINT_NONE" in dmesg
            ):
                test_results.append(
                    Result(
                        name=OOM_IN_DMESG_TEST_NAME, status=Result.StatusExtended.FAIL
                    )
                )

    if not info.is_local_run:
        failed_suits = []
        for test_result in test_results:
            if not test_result.is_ok() and ".py" in test_result.name:
                failed_suits.append(test_result.name.split("/")[0])
        failed_suits = list(set(failed_suits))
        for failed_suit in failed_suits:
            files.extend(
                Shell.get_output(
                    f"find ./tests/integration/{failed_suit} -name '*.log*'"
                ).splitlines()
            )

        files = [Utils.compress_files_gz(files, f"{temp_path}/logs.tar.gz")]

    R = Result.create_from(results=test_results, stopwatch=sw, files=files)

    if has_error:
        R.set_error().set_info("\n".join(error_info))

    if is_bugfix_validation:
        has_failure = False
        for r in R.results:
            # invert statuses
            r.set_label("xfail")
            if r.status == Result.StatusExtended.FAIL:
                r.status = Result.StatusExtended.OK
                has_failure = True
            elif r.status == Result.StatusExtended.OK:
                r.status = Result.StatusExtended.FAIL
        if not has_failure:
            print("Failed to reproduce the bug")
            R.set_failed()
            R.set_info("Failed to reproduce the bug")
        else:
            R.set_success()

    R.sort().complete_job()


if __name__ == "__main__":
    main()
