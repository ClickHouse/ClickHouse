from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Utils

temp_path = Utils.temp_path()

def finalize_pytest_result(result: Result) -> Result:
    """
    Add logs for failed tests.
    """
    if result.is_ok():
        return result

    # Collect docker compose configs used in tests
    config_files = [
        str(p)
        for p in Path(f"{temp_path}/tests/integration/").glob("test_*/_instances*/*/configs/")
    ]
    attached_files = [
        Utils.compress_files_gz(config_files, f"{temp_path}/configs.tar.gz")
    ]
    result.set_files(attached_files)
    return result

    test_results = []
    failed_tests_files = []

    has_error = False
    error_info = []

    module_repeat_cnt = 1
    if is_flaky_check:
        module_repeat_cnt = FLAKY_CHECK_MODULE_REPEAT_COUNT

    failed_test_cases = []

    if parallel_test_modules:
        for attempt in range(module_repeat_cnt):
            log_file = f"{temp_path}/pytest_parallel.log"
            test_result_parallel = Result.from_pytest_run(
                command=f"{' '.join(parallel_test_modules)} --report-log-exclude-logs-on-passed-tests -n {workers} --dist=loadfile --tb=short {repeat_option} --session-timeout=5400",
                cwd="./tests/integration/",
                env=test_env,
                pytest_report_file=f"{temp_path}/pytest_parallel.jsonl",
                logfile=log_file,
            )
            if is_flaky_check and not test_result_parallel.is_ok():
                print(
                    f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                )
                break
        test_results.extend(test_result_parallel.results)
        failed_test_cases.extend(
            [t.name for t in test_result_parallel.results if t.is_failure()]
        )
        if test_result_parallel.files:
            failed_tests_files.extend(test_result_parallel.files)
        if test_result_parallel.is_error():
            has_error = True
            error_info.append(test_result_parallel.info)

    fail_num = len([r for r in test_results if not r.is_ok()])
    if sequential_test_modules and fail_num < MAX_FAILS_BEFORE_DROP and not has_error:
        for attempt in range(module_repeat_cnt):
            log_file = f"{temp_path}/pytest_sequential.log"
            test_result_sequential = Result.from_pytest_run(
                command=f"{' '.join(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests --tb=short {repeat_option} -n 1 --dist=loadfile --session-timeout=5400",
                env=test_env,
                cwd="./tests/integration/",
                pytest_report_file=f"{temp_path}/pytest_sequential.jsonl",
                logfile=log_file,
            )
            if is_flaky_check and not test_result_sequential.is_ok():
                print(
                    f"Flaky check: Test run fails after attempt [{attempt+1}/{module_repeat_cnt}] - break"
                )
                break
        test_results.extend(test_result_sequential.results)
        failed_test_cases.extend(
            [t.name for t in test_result_sequential.results if t.is_failure()]
        )
        if test_result_sequential.files:
            failed_tests_files.extend(test_result_sequential.files)
        if test_result_sequential.is_error():
            has_error = True
            error_info.append(test_result_sequential.info)

    # Collect logs before rerun
    attached_files = []
    if not info.is_local_run:
        failed_suits = []
        for test_result in test_results:
            if not test_result.is_ok() and ".py" in test_result.name:
                failed_suits.append(test_result.name.split("/")[0])
        failed_suits = list(set(failed_suits))
        for failed_suit in failed_suits:
            failed_tests_files.append(f"tests/integration/{failed_suit}")

        if failed_suits:
            attached_files.append(
                Utils.compress_files_gz(failed_tests_files, f"{temp_path}/logs.tar.gz")
            )
            attached_files.append(
                Utils.compress_files_gz(config_files, f"{temp_path}/configs.tar.gz")
            )

    if Path(f"{temp_path}/ci/tmp/docker-in-docker.log").exists():
        attached_files.append(f"{temp_path}/ci/tmp/docker-in-docker.log")

    return result