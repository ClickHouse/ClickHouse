import argparse
import os
import subprocess
import time
from pathlib import Path
from typing import List, Tuple

from more_itertools import tail

from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.integration_tests_configs import (
    IMAGES_ENV,
    LLVM_COVERAGE_SKIP_PREFIXES,
    get_optimal_test_batch,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"
MAX_FAILS_BEFORE_DROP = 5
OOM_IN_DMESG_TEST_NAME = "OOM in dmesg"
ncpu = Utils.cpu_count()
mem_gb = round(Utils.physical_memory() // (1024**3), 1)

MAX_CPUS_PER_WORKER = 5
MAX_MEM_PER_WORKER = 11

INFRASTRUCTURE_ERROR_PATTERNS = [
    "timed out after",
    "TimeoutExpired",
    "Cannot connect to the Docker daemon",
    "Error response from daemon",
    "Name or service not known",
    "Temporary failure in name resolution",
    "Network is unreachable",
    "Connection reset by peer",
    "No space left on device",
    "Cannot allocate memory",
    "OCI runtime create failed",
    "toomanyrequests",
    "pull access denied",
]


def _is_infrastructure_error(result: Result) -> bool:
    """Returns True if the result is an ERROR caused by infrastructure issues."""
    if result.status not in (Result.Status.ERROR, Result.StatusExtended.ERROR):
        return False
    if not result.info:
        return False
    return any(pattern in result.info for pattern in INFRASTRUCTURE_ERROR_PATTERNS)


def _mark_infrastructure_errors(results: list) -> int:
    """Scan results, label infrastructure errors with INFRA and change their status to SKIPPED.

    Returns the number of results that were relabeled.
    """
    count = 0
    for r in results:
        if _is_infrastructure_error(r):
            r.set_label(Result.Label.INFRA)
            r.status = Result.StatusExtended.SKIPPED
            count += 1
    if count:
        print(f"Marked {count} test result(s) as infrastructure errors")
    return count


def _start_docker_in_docker():
    with open("./ci/tmp/docker-in-docker.log", "w") as log_file:
        dockerd_proc = subprocess.Popen(
            "./ci/jobs/scripts/docker_in_docker.sh",
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    retries = 20
    for i in range(retries):
        # On last retry, show errors; otherwise suppress them
        cmd = "docker info > /dev/null" if i == retries - 1 else "docker info > /dev/null 2>&1"
        if Shell.check(cmd, verbose=True):
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
    parser.add_argument(
        "--workers",
        help="Optional. Number of parallel workers for pytest",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--session-timeout",
        help="Optional. Session timeout in seconds",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--param",
        help=(
            "Optional. Comma-separated KEY=VALUE pairs to inject as environment "
            "variables for pytest (e.g. --param PYTEST_ADDOPTS=-vv,CUSTOM_FLAG=1)"
        ),
        type=str,
        default="",
    )
    return parser.parse_args()


def merge_profraw_files(llvm_profdata_cmd: str, batch_num: int):
    """Merge all profraw files into final profdata file.

    Args:
        llvm_profdata_cmd: Path to llvm-profdata tool
        batch_num: Batch number for naming output file
    """
    import subprocess
    from pathlib import Path

    # Find all profraw files
    profraw_files = [str(p) for p in Path(".").rglob("*.profraw")]

    if not profraw_files:
        print("No profraw files found", flush=True)
        return

    final_file = f"./it-{batch_num}.profdata"
    print(f"Merging {len(profraw_files)} profraw files into {final_file}", flush=True)

    result = subprocess.run(
        [llvm_profdata_cmd, "merge", "-sparse", "-failure-mode=warn"]
        + profraw_files
        + ["-o", final_file],
        capture_output=True,
        text=True,
    )

    # Check for corrupted files in stderr
    corrupted_count = result.stderr.count(
        "invalid instrumentation profile"
    ) + result.stderr.count("file header is corrupt")
    if corrupted_count > 0:
        print(f"  WARNING: Found {corrupted_count} corrupted profraw files", flush=True)
        # Extract and display corrupted filenames from stderr
        corrupted_files = set()
        for line in result.stderr.split("\n"):
            if (
                "invalid instrumentation profile" in line
                or "file header is corrupt" in line
                or "error:" in line.lower()
            ):
                print(f"    {line.strip()}", flush=True)
                # Extract filename from error message (format: "error: file.profraw: ..." or "warning: file.profraw: ...")
                parts = line.split(":")
                if len(parts) >= 2:
                    potential_file = parts[1].strip()
                    if potential_file.endswith(".profraw"):
                        corrupted_files.add(potential_file)
        if corrupted_files:
            print(f"  Corrupted files: {', '.join(corrupted_files)}", flush=True)

    if result.returncode == 0:
        print(f"Successfully created final coverage file: {final_file}", flush=True)

        # Delete merged profraw files to save disk space
        deleted_count = 0
        for profraw_file in profraw_files:
            try:
                Path(profraw_file).unlink()
                deleted_count += 1
            except Exception as e:
                print(f"  WARNING: Failed to delete {profraw_file}: {e}", flush=True)
        print(f"  Deleted {deleted_count} profraw files", flush=True)
    else:
        print(f"ERROR: Failed to create final coverage file", flush=True)
        if result.stderr:
            print(result.stderr, flush=True)


FLAKY_CHECK_TEST_REPEAT_COUNT = 3
FLAKY_CHECK_MODULE_REPEAT_COUNT = 2


def get_parallel_sequential_tests_to_run(
    batch_num: int,
    total_batches: int,
    args_test: List[str],
    workers: int,
    job_options: str,
    info: Info,
    no_strict: bool = False,
) -> Tuple[List[str], List[str]]:
    if args_test:
        batch_num = 1
        total_batches = 1

    test_files = [
        str(p.relative_to("./tests/integration/"))
        for p in Path("./tests/integration/").glob("test_*/test*.py")
    ]

    if "amd_llvm_coverage" in (job_options or ""):
        before = len(test_files)
        test_files = [
            f
            for f in test_files
            if not any(f.startswith(prefix) for prefix in LLVM_COVERAGE_SKIP_PREFIXES)
        ]
        print(
            f"LLVM coverage: skipped {before - len(test_files)} test files matching LLVM_COVERAGE_SKIP_PREFIXES"
        )

    assert len(test_files) > 100

    parallel_test_modules, sequential_test_modules = get_optimal_test_batch(
        test_files, total_batches, batch_num, workers, job_options, info
    )
    if not args_test:
        return parallel_test_modules, sequential_test_modules

    # there are following possible values for args.test:
    # 1) test suit (e.g. test_directory or test_directory/)
    # 2) test module (e.g. test_directory/test_module or test_directory/test_module.py)
    # 3) test case (e.g. test_directory/test_module.py::test_case or test_directory/test_module::test_case[test_param])
    def normalize_test_path(test_arg: str) -> str:
        """Normalize test path by removing integration test directory prefixes."""
        # Handle: tests/integration/, integration/, ./tests/integration/, or full paths
        if "tests/integration/" in test_arg:
            # Extract everything after tests/integration/
            test_arg = test_arg.split("tests/integration/", 1)[1]
        elif test_arg.startswith("integration/"):
            # Handle integration/ prefix
            test_arg = test_arg[len("integration/"):]
        return test_arg

    def test_match(test_file: str, test_arg: str) -> bool:
        if "/" not in test_arg:
            return f"{test_arg}/" in test_file
        if test_arg.endswith(".py"):
            return test_file == test_arg
        test_arg = test_arg.split("::", maxsplit=1)[0]
        return test_file.removesuffix(".py") == test_arg.removesuffix(".py")

    parallel_tests = []
    sequential_tests = []
    for test_arg in args_test:
        # Normalize the test path first
        normalized_test_arg = normalize_test_path(test_arg)
        matched = False
        for test_file in parallel_test_modules:
            if test_match(test_file, normalized_test_arg):
                parallel_tests.append(normalized_test_arg)
                matched = True
        for test_file in sequential_test_modules:
            if test_match(test_file, normalized_test_arg):
                sequential_tests.append(normalized_test_arg)
                matched = True
        if not no_strict:
            assert matched, f"Test [{test_arg}] not found"

    return parallel_tests, sequential_tests


def tail(filepath: str, buff_len: int = 1024) -> List[str]:
    with open(filepath, "rb") as f:
        # Get file size to avoid seeking before start of file
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        if file_size <= buff_len:
            # File is smaller than buffer, read from beginning
            f.seek(0)
        else:
            # File is larger, seek from end
            f.seek(-buff_len, os.SEEK_END)
            f.readline()  # Skip partial line

        data = f.read()
        return data.decode(errors="replace")


def run_pytest_and_collect_results(
    command: str, env: str, report_name: str, timeout: int = None
) -> Result:
    """
    Does xdist timeout check.
    """

    test_result = Result.from_pytest_run(
        command=command,
        env=env,
        cwd="./tests/integration/",
        pytest_report_file=f"{temp_path}/pytest_{report_name}.jsonl",
        pytest_logfile=f"{temp_path}/pytest_{report_name}.log",
        logfile=f"{temp_path}/{report_name}.log",
        timeout=timeout,
    )

    if "!!!!!!! xdist.dsession.Interrupted: session-timeout:" in tail(
        f"{temp_path}/{report_name}.log"
    ):
        test_result.info = "ERROR: session-timeout occurred during test execution"
        assert test_result.status == Result.Status.ERROR
        test_result.results.append(
            Result(
                name="Timeout",
                status=Result.StatusExtended.FAIL,
                info=test_result.info,
            )
        )
    return test_result


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    job_params = args.options.split(",") if args.options else []
    job_params = [to.strip() for to in job_params]
    use_old_analyzer = False
    use_distributed_plan = False
    use_database_disk = False
    is_flaky_check = False
    is_bugfix_validation = False
    is_parallel = False
    is_sequential = False
    is_targeted_check = False
    is_llvm_coverage = False
    llvm_profdata_cmd = None

    # Set on_error_hook to collect logs on hard timeout
    Result.from_fs(info.job_name).set_on_error_hook(
        """
dmesg -T >./ci/tmp/dmesg.log
sudo chown -R $(id -u):$(id -g) ./tests/integration
tar -czf ./ci/tmp/logs.tar.gz \
  ./tests/integration/test_*/_instances*/ \
  ./ci/tmp/*.log \
  ./ci/tmp/*.jsonl || :
"""
    ).set_files(
        [
            "./ci/tmp/logs.tar.gz",
            "./ci/tmp/dmesg.log",
            "./ci/tmp/docker-in-docker.log",
        ],
        strict=False,
    )

    if args.param:
        for item in args.param.split(","):
            print(f"Setting env variable: {item}")
            key, _, value = item.partition("=")
            key = key.strip()
            if not key:
                continue
            os.environ[key] = value.strip()

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
            if "amd_llvm_coverage" in to:
                is_llvm_coverage = True
        elif to == "old analyzer":
            use_old_analyzer = True
        elif to == "distributed plan":
            use_distributed_plan = True
        elif to == "db disk":
            use_database_disk = True
        elif to == "flaky":
            is_flaky_check = True
        elif to == "parallel":
            is_parallel = True
        elif to == "sequential":
            is_sequential = True
        elif "bugfix" in to.lower() or "validation" in to.lower():
            is_bugfix_validation = True
        elif "targeted" in to:
            is_targeted_check = True
        else:
            assert False, f"Unknown job option [{to}]"

    if args.count or is_flaky_check:
        repeat_option = (
            f"--count {args.count or FLAKY_CHECK_TEST_REPEAT_COUNT} --random-order"
        )
    elif is_targeted_check:
        repeat_option = f"--count 10 --random-order"

    if args.workers:
        workers = args.workers
    else:
        print("ncpu:", ncpu)
        print("mem_gb:", mem_gb)
        workers = min(ncpu // MAX_CPUS_PER_WORKER, mem_gb // MAX_MEM_PER_WORKER) or 1

    clickhouse_path = f"{Utils.cwd()}/ci/tmp/clickhouse"
    clickhouse_server_config_dir = f"{Utils.cwd()}/programs/server"
    if info.is_local_run:
        if args.path:
            clickhouse_path = args.path
        else:
            paths_to_check = [
                clickhouse_path,  # it's set for CI runs, but we need to check it
                f"{Utils.cwd()}/build/programs/clickhouse",
                f"{Utils.cwd()}/clickhouse",
            ]
            for path in paths_to_check:
                if Path(path).is_file():
                    clickhouse_path = path
                    break
            else:
                raise FileNotFoundError(
                    "Clickhouse binary not found in any of the paths: "
                    + ", ".join(paths_to_check)
                    + ". You can also specify path to binary via --path argument"
                )
        if args.path_1:
            clickhouse_server_config_dir = args.path_1
    assert Path(
        clickhouse_server_config_dir
    ), f"Clickhouse config dir does not exist [{clickhouse_server_config_dir}]"
    print(f"Using ClickHouse binary at [{clickhouse_path}]")

    changed_test_modules = []
    if is_bugfix_validation or is_flaky_check or is_targeted_check:
        if info.is_local_run:
            assert (
                args.test
            ), "--test must be provided for flaky or bugfix job flavor with local run"
        else:
            if is_bugfix_validation and Labels.PR_BUGFIX not in info.pr_labels:
                # Not a bugfix PR - run a simple sanity test
                changed_test_modules = ["test_accept_invalid_certificate/test.py"]
            else:
                # TODO: reduce scope to modified test cases instead of entire modules
                changed_files = info.get_changed_files()
                for file in changed_files:
                    if (
                        file.startswith("tests/integration/test")
                        and Path(file).name.startswith("test")
                        and file.endswith(".py")
                        and Path(file).is_file()
                    ):
                        changed_test_modules.append(
                            file.removeprefix("tests/integration/")
                        )

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

    if is_bugfix_validation or is_flaky_check:
        assert (
            changed_test_modules
        ), "No changed test modules found, either job must be skipped or bug in changed test search logic"

    Shell.check(f"chmod +x {clickhouse_path}", verbose=True, strict=True)
    Shell.check(f"{clickhouse_path} --version", verbose=True, strict=True)

    targeted_tests = []
    if is_targeted_check:
        assert not args.test, "--test not supposed to be used for targeted check ???"
        targeter = Targeting(info=info)
        tests, results_with_info = targeter.get_all_relevant_tests_with_info(
            clickhouse_path
        )
        # no subtask level for integration tests - cannot add this info to the report now
        # results.append(results_with_info)
        if not tests:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No failed tests found from previous runs",
            ).complete_job()

        # Parse test names from the query result
        for test_ in tests:
            if test_.strip():
                test_name = test_.strip()
                targeted_tests.append(
                    test_name.split("[")[0]
                )  # remove parametrization - does not work with test repeat with --count
        print(f"Parsed {len(targeted_tests)} test names: {targeted_tests}")

    if not Shell.check("docker info > /dev/null 2>&1", verbose=True):
        _start_docker_in_docker()
    Shell.check("docker info > /dev/null", verbose=True, strict=True)

    parallel_test_modules, sequential_test_modules = (
        get_parallel_sequential_tests_to_run(
            batch_num,
            total_batches,
            args.test or targeted_tests or changed_test_modules,
            workers,
            args.options,
            info,
            no_strict=is_targeted_check,  # targeted check might want to run test that was removed on a merge-commit
        )
    )

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
        "CLICKHOUSE_USE_DATABASE_DISK": "1" if use_database_disk else "0",
        "PYTEST_CLEANUP_CONTAINERS": "1",
        "JAVA_PATH": java_path,
    }
    if is_llvm_coverage:
        test_env["LLVM_PROFILE_FILE"] = f"it-%4m.profraw"
        print(
            f"NOTE: This is LLVM coverage run, setting LLVM_PROFILE_FILE to [{test_env['LLVM_PROFILE_FILE']}]"
        )
        # Auto-detect available LLVM profdata tool
        for ver in ["21", "20", "18", "19", "17", "16", ""]:
            cmd = f"llvm-profdata{'-' + ver if ver else ''}"
            if Shell.check(f"command -v {cmd}", verbose=False):
                llvm_profdata_cmd = cmd
                break

        if not llvm_profdata_cmd:
            print("ERROR: llvm-profdata not found in PATH")
        else:
            print(f"Using {llvm_profdata_cmd} to merge coverage files")

    test_results = []
    failed_tests_files = []

    has_error = False
    session_timeout_parallel = 3600 * 2
    session_timeout_sequential = 3600

    if is_llvm_coverage:
        session_timeout_parallel = 7200
        session_timeout_sequential = 7200

    if args.session_timeout:
        session_timeout_parallel = args.session_timeout * 2
        session_timeout_sequential = args.session_timeout

    error_info = []

    module_repeat_cnt = 1
    if is_flaky_check:
        module_repeat_cnt = FLAKY_CHECK_MODULE_REPEAT_COUNT

    failed_test_cases = []

    if parallel_test_modules:
        for attempt in range(module_repeat_cnt):
            log_file = f"{temp_path}/pytest_parallel.log"
            test_result_parallel = run_pytest_and_collect_results(
                command=f"{' '.join(parallel_test_modules)} --report-log-exclude-logs-on-passed-tests -n {workers} --dist=loadfile --tb=short {repeat_option} --session-timeout={session_timeout_parallel}",
                env=test_env,
                report_name="parallel",
                timeout=session_timeout_parallel + 600,
            )
            if is_flaky_check and not test_result_parallel.is_ok():
                print(
                    f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                )
                break
        test_results.extend(test_result_parallel.results)
        _mark_infrastructure_errors(test_result_parallel.results)
        failed_test_cases.extend(
            [t.name for t in test_result_parallel.results if t.is_failure()]
        )
        if test_result_parallel.files:
            failed_tests_files.extend(test_result_parallel.files)
        if test_result_parallel.is_error():
            if not is_targeted_check:
                # In targeted checks we may overload the run with many or heavy tests
                # (--count N is used). In this mode, a session-timeout is an expected risk
                # rather than an infrastructure problem, so we do not treat such errors as job-level
                # failures and avoid setting the error flag for targeted runs.
                has_error = True
                error_info.append(test_result_parallel.info)

    fail_num = len([r for r in test_results if not r.is_ok()])
    if sequential_test_modules and fail_num < MAX_FAILS_BEFORE_DROP and not has_error:
        for attempt in range(module_repeat_cnt):
            test_result_sequential = run_pytest_and_collect_results(
                command=f"{' '.join(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests --tb=short {repeat_option} -n 1 --dist=loadfile --session-timeout={session_timeout_sequential}",
                env=test_env,
                report_name="sequential",
                timeout=session_timeout_sequential + 600,
            )

            if is_flaky_check and not test_result_sequential.is_ok():
                print(
                    f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                )
                break
        test_results.extend(test_result_sequential.results)
        _mark_infrastructure_errors(test_result_sequential.results)
        failed_test_cases.extend(
            [t.name for t in test_result_sequential.results if t.is_failure()]
        )
        if test_result_sequential.files:
            failed_tests_files.extend(test_result_sequential.files)
        if test_result_sequential.is_error():
            if not is_targeted_check:
                # In targeted checks we may overload the run with many or heavy tests
                # (--count N is used). In this mode, a session-timeout is an expected risk
                # rather than an infrastructure problem, so we do not treat such errors as job-level
                # failures and avoid setting the error flag for targeted runs.
                has_error = True
                error_info.append(test_result_sequential.info)

    # Collect logs before re-run
    attached_files = []
    if not info.is_local_run:
        failed_suits = []
        # Collect docker compose configs used in tests
        config_files = [
            str(p)
            for p in Path("./tests/integration/").glob("test_*/_instances*/*/configs/")
        ]
        for test_result in test_results:
            if not test_result.is_ok() and ".py" in test_result.name:
                failed_suits.append(test_result.name.split("/")[0])
        failed_suits = list(set(failed_suits))
        for failed_suit in failed_suits:
            failed_tests_files.append(f"tests/integration/{failed_suit}")

        # Add all files matched ./ci/tmp/*.log ./ci/tmp/*.jsonl into failed_tests_files
        for pattern in ["*.log", "*.jsonl"]:
            for log_file in Path("./ci/tmp/").glob(pattern):
                if log_file.is_file():
                    failed_tests_files.append(str(log_file))

        if failed_suits:
            attached_files.append(
                Utils.compress_files_gz(failed_tests_files, f"{temp_path}/logs.tar.gz")
            )
            attached_files.append(
                Utils.compress_files_gz(config_files, f"{temp_path}/configs.tar.gz")
            )
            if Path("./ci/tmp/docker-in-docker.log").exists():
                attached_files.append("./ci/tmp/docker-in-docker.log")

    # Rerun failed tests if any to check if failure is reproducible
    if 0 < len(failed_test_cases) < 10 and not (
        is_flaky_check or is_bugfix_validation or is_targeted_check or info.is_local_run
    ):
        test_result_retries = run_pytest_and_collect_results(
            command=f"{' '.join(failed_test_cases)} --report-log-exclude-logs-on-passed-tests --tb=short -n 1 --dist=loadfile --session-timeout=1200",
            env=test_env,
            report_name="retries",
            timeout=1200 + 600,
        )
        successful_retries = [t.name for t in test_result_retries.results if t.is_ok()]
        failed_retries = [t.name for t in test_result_retries.results if t.is_failure()]
        if successful_retries or failed_retries:
            for test_case in test_results:
                if test_case.name in successful_retries:
                    test_case.set_label(Result.Label.OK_ON_RETRY)
                elif test_case.name in failed_retries:
                    test_case.set_label(Result.Label.FAILED_ON_RETRY)

    # Remove iptables rule added in tests
    Shell.check("sudo iptables -D DOCKER-USER 1 ||:", verbose=True)

    if not info.is_local_run:
        print("Dumping dmesg")
        Shell.check("dmesg -T > ./ci/tmp/dmesg.log", verbose=True, strict=True)
        with open("./ci/tmp/dmesg.log", "rb") as dmesg:
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
                attached_files.append("./ci/tmp/dmesg.log")

    # For targeted checks, session-timeout is an expected risk (because of --count N
    # overloading), so do not propagate the synthetic "Timeout" result as a failure.
    if is_targeted_check:
        test_results = [r for r in test_results if r.name != "Timeout"]

    R = Result.create_from(results=test_results, stopwatch=sw, files=attached_files)

    if is_llvm_coverage:
        assert (
            is_bugfix_validation is False
        ), "LLVM coverage with bugfix validation is not supported"
        has_failure = False
        for r in R.results:
            if r.status == Result.StatusExtended.FAIL:
                if r.has_label(Result.Label.OK_ON_RETRY):
                    # Remove label and set to OK
                    r.remove_label(Result.Label.OK_ON_RETRY)
                    r.status = Result.StatusExtended.OK
                else:
                    has_failure = True
        if has_failure:
            R.set_failed()
            R.set_info("Some tests failed during LLVM coverage run")
        else:
            R.set_success()
            has_error = False

    # If all non-OK results are infrastructure errors, do not treat as a real failure
    if has_error:
        non_ok = [r for r in test_results if not r.is_ok()]
        if non_ok and all(r.has_label(Result.Label.INFRA) for r in non_ok):
            print(
                "All failures are infrastructure errors - clearing error flag"
            )
            has_error = False
            force_ok_exit = True

    if has_error:
        R.set_error().set_info("\n".join(error_info))

    if is_bugfix_validation and Labels.PR_BUGFIX in info.pr_labels:
        assert (
            is_llvm_coverage is False
        ), "Bugfix validation with LLVM coverage is not supported"
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

    force_ok_exit = False
    if is_llvm_coverage and llvm_profdata_cmd:
        print("Collecting and merging LLVM coverage files...")

        # Merge all profraw files into final profdata file
        merge_profraw_files(llvm_profdata_cmd, batch_num)

        force_ok_exit = True
        print("NOTE: LLVM coverage job - do not block pipeline - exit with 0")

    R.sort().complete_job(do_not_block_pipeline_on_failure=force_ok_exit)


if __name__ == "__main__":
    main()
