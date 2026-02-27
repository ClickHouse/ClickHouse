import argparse
import os
import random
import subprocess
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.functional_tests.export_coverage import CoverageExporter
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"

class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"
    RETRIES = "retries"
    CHECK_ERRORS = "check_errors"
    COLLECT_LOGS = "collect_logs"
    COLLECT_COVERAGE = "collect_coverage"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run ClickHouse functional tests (CI job)"
    )
    parser.add_argument(
        "--options",
        help="Comma-separated options. Examples: parallel|sequential|BATCH_NUM/BATCH_TOT|s3 storage|DatabaseReplicated|azure|AsyncInsert|BugfixValidation|coverage",
        default="",
    )
    parser.add_argument(
        "--param",
        help="Optional start stage: install|start|test|check_errors|collect_logs|collect_coverage",
        default=None,
    )
    parser.add_argument(
        "--test",
        help="Optional. Space-separated test name patterns",
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
        "--path",
        help="Optional. Path to a directory that contains the clickhouse binary",
        type=str,
        default="",
    )
    parser.add_argument(
        "--workers",
        help="Optional. Number of parallel workers for the test runner. Default: automatically computed from CPU count and job type",
        default=None,
    )
    parser.add_argument(
        "--debug",
        help="Optional. Open clickhouse-client console after test run",
        default=False,
        action="store_true",
    )
    return parser.parse_args()


def run_tests(
    batch_num: int,
    batch_total: int,
    tests: list[str] = None,
    extra_args="",
    rerun_count=1,
    random_order=False,
    global_time_limit=0,
):
    test_output_file = f"{temp_dir}/test_result.txt"
    if batch_num and batch_total:
        extra_args += (
            f" --run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
        )
    if "--no-shard" not in extra_args:
        extra_args += " --shard"
    if "--no-zookeeper" not in extra_args:
        extra_args += " --zookeeper"
    # Remove --report-logs-stats, it hides sanitizer errors in def reportLogStats(args): clickhouse_execute(args, "SYSTEM FLUSH LOGS")
    global_time_limit_option = (
        f"--global_time_limit={global_time_limit}" if global_time_limit > 0 else ""
    )
    command = f"clickhouse-test --testname --check-zookeeper-session --hung-check --memory-limit {5*2**30} --trace \
                --capture-client-stacktrace --queries ./tests/queries --test-runs {rerun_count} \
                {extra_args} {global_time_limit_option} \
                --queries ./tests/queries {('--order=random' if random_order else '')} -- {' '.join(tests) if tests else ''} | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(command, verbose=True)


OPTIONS_TO_INSTALL_ARGUMENTS = {
    "old analyzer": "--analyzer",
    "WasmEdge": "--wasm-engine wasmedge",
    "s3 storage": "--s3-storage",
    "DatabaseReplicated": "--db-replicated",
    "DatabaseOrdinary": "--db-ordinary",
    "wide parts enabled": "--wide-parts",
    "ParallelReplicas": "--parallel-rep",
    "distributed plan": "--distributed-plan",
    "azure": "--azure",
    "AsyncInsert": " --async-insert",
    "BugfixValidation": " --bugfix-validation",
    "db disk": "--remote-database-disk",
}

OPTIONS_TO_TEST_RUNNER_ARGUMENTS = {
    "s3 storage": "--s3-storage --no-stateful",
    "ParallelReplicas": "--no-zookeeper --no-shard --no-parallel-replicas",
    "AsyncInsert": " --no-async-insert",
    "DatabaseReplicated": " --no-stateful --replicated-database",
    "azure": " --azure-blob-storage --no-random-settings --no-random-merge-tree-settings",  # azurite is slow, with randomization it can be super slow
    "parallel": "--no-sequential",
    "sequential": "--no-parallel",
    "flaky check": "--flaky-check",
    "targeted": "--flaky-check",  # to disable tests not compatible with the thread fuzzer
}


def main():
    args = parse_args()
    test_options = [to.strip() for to in args.options.split(",")]
    batch_num, total_batches = 0, 0
    config_installs_args = ""
    is_flaky_check = False
    is_targeted_check = False
    is_bugfix_validation = False
    is_s3_storage = False
    is_azure_storage = False
    is_database_replicated = False
    is_shared_catalog = False
    is_encrypted_storage = random.choice([True, False])
    is_parallel_replicas = False
    is_llvm_coverage = False
    is_coverage = False
    runner_options = ""
    # optimal value for most of the jobs
    nproc = int(Utils.cpu_count() * 0.6)
    info = Info()

    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif to in OPTIONS_TO_INSTALL_ARGUMENTS:
            pass
        elif to.startswith("amd_") or to.startswith("arm_"):
            pass
        elif to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            pass
        else:
            assert False, f"Unknown option [{to}]"

        if to in OPTIONS_TO_INSTALL_ARGUMENTS:
            print(f"NOTE: Enabled config option [{OPTIONS_TO_INSTALL_ARGUMENTS[to]}]")
            config_installs_args += f" {OPTIONS_TO_INSTALL_ARGUMENTS[to]}"

        if to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            if to in ("parallel", "sequential") and args.test:
                # skip setting up parallel/sequential if specific tests are provided
                continue
            else:
                runner_options += f" {OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}"
                print(
                    f"NOTE: Enabled test runner option [{OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}]"
                )

        if "targeted" in to:
            is_targeted_check = True
        elif "flaky" in to:
            is_flaky_check = True
        elif "BugfixValidation" in to:
            is_bugfix_validation = True
        elif "amd_llvm_coverage" in to:
            is_llvm_coverage = True
        elif "coverage" in to:
            is_coverage = True
        if "s3 storage" in to:
            is_s3_storage = True
        if "azure" in to:
            is_azure_storage = True
        if "DatabaseReplicated" in to:
            is_database_replicated = True
        if "SharedCatalog" in to:
            is_shared_catalog = True
        if "ParallelReplicas" in to:
            is_parallel_replicas = True

    if is_shared_catalog or is_parallel_replicas:
        pass
    else:
        if "binary" in args.options and len(test_options) < 3:
            # Plain binary job runs fast; allow higher concurrency
            nproc = int(Utils.cpu_count() * 1.2)
        elif is_database_replicated:
            nproc = int(Utils.cpu_count() * 0.4)
        elif "msan" in args.options:
            # MSan is slow
            nproc = int(Utils.cpu_count() * 0.4)
        elif is_coverage:
            cidb_cluster = CIDBCluster()
            assert cidb_cluster.is_ready()
            nproc = 1
        else:
            pass

    workers = None
    if args.workers:
        print(f"Workers count set from --workers: {args.workers}")
        workers = args.workers
    else:
        print(f"Workers count set to optimal value: {nproc}")
        workers = nproc

    runner_options += f" --jobs {workers}"

    if is_llvm_coverage:
        # Randomization makes coverage non-deterministic, long tests are slow to collect coverage
        runner_options += " --no-random-settings --no-random-merge-tree-settings --no-long --llvm-coverage"
        os.environ["LLVM_PROFILE_FILE"] = f"ft-{batch_num}-%2m.profraw"

    rerun_count = 1
    if args.count:
        print(f"Rerun count set from --count: {args.count}")
        rerun_count = args.count
    elif is_flaky_check:
        print(f"Rerun count set to 50 for flaky check")
        rerun_count = 50
    elif is_targeted_check:
        print(f"Rerun count set to 5 for targeted check")
        rerun_count = 5

    if not info.is_local_run:
        # TODO: find a way to work with Azure secret so it's ok for local tests as well, for now keep azure disabled
        azure_connection_string = Shell.get_output(
            f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
            verbose=True,
            strict=True,
        )
        os.environ["AZURE_CONNECTION_STRING"] = azure_connection_string
    else:
        print("Disable azure for a local run")
        config_installs_args += " --no-azure"

    if (is_azure_storage or is_s3_storage) and is_encrypted_storage:
        config_installs_args += " --encrypted-storage"
        runner_options += f" --encrypted-storage"

    if is_bugfix_validation:
        os.environ["GLOBAL_TAGS"] = "no-random-settings"
        ch_path = temp_dir
        if not info.is_local_run or not (Path(temp_dir) / "clickhouse").is_file():
            link_arch = "aarch64" if Utils.is_arm() else "amd64"
            link_to_master_head_binary = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/{link_arch}/clickhouse"
            Shell.run(
                f"wget -nv -P {temp_dir} {link_to_master_head_binary}",
                verbose=True,
                strict=True,
            )
    elif args.path:
        assert Path(args.path).is_dir(), f"Path [{args.path}] is not a directory"
        ch_path = str(Path(args.path).absolute())
    else:
        paths_to_check = [
            f"{temp_dir}/clickhouse",  # it's set for CI runs, but we need to check it
            f"{Utils.cwd()}/build/programs/clickhouse",
            f"{Utils.cwd()}/clickhouse",
        ]
        for path in paths_to_check:
            if Path(path).is_file():
                ch_path = str(Path(path).parent.absolute())
                break
        else:
            raise FileNotFoundError(
                "Clickhouse binary not found in any of the paths: "
                + ", ".join(paths_to_check)
                + ". You can also specify path to binary via --path argument"
            )

    Shell.check(f"chmod +x {ch_path}/clickhouse")

    stop_watch = Utils.Stopwatch()

    res = True
    results = []
    debug_files = []

    stages = list(JobStages)
    if not is_coverage:
        stages.remove(JobStages.COLLECT_COVERAGE)
    else:
        stages.remove(JobStages.COLLECT_LOGS)
    if is_coverage or info.is_local_run or is_bugfix_validation:
        # For bugfix validation, we intentionally skip the check error stage (checks FATAL messages):
        # regular test failures are assumed to be sufficient to validate the test
        stages.remove(JobStages.CHECK_ERRORS)
    if info.is_local_run:
        if JobStages.COLLECT_LOGS in stages:
            stages.remove(JobStages.COLLECT_LOGS)
        if JobStages.COLLECT_COVERAGE in stages:
            stages.remove(JobStages.COLLECT_COVERAGE)
    if (
        is_flaky_check
        or is_coverage
        or is_bugfix_validation
        or is_targeted_check
        or info.is_local_run
    ):
        stages.remove(JobStages.RETRIES)

    tests = args.test

    # for local run check if stateful tests are present to skip prepare_stateful_data and start faster if not
    has_stateful_tests = True
    if tests and info.is_local_run:
        from glob import glob

        has_stateful = False
        for test_pattern in tests:
            test_pattern_clean = test_pattern.strip()
            matching_files = glob(
                f"tests/queries/**/*{test_pattern_clean}*.sql", recursive=True
            )
            matching_files += glob(
                f"tests/queries/**/*{test_pattern_clean}*.sh", recursive=True
            )
            for test_file in matching_files:
                try:
                    with open(test_file, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if "stateful" in content.lower():
                            has_stateful = True
                            break
                except Exception:
                    pass
            if has_stateful:
                break
        if not has_stateful:
            has_stateful_tests = False

    targeter = Targeting(info=info)
    if is_flaky_check or is_bugfix_validation:
        if info.is_local_run:
            assert (
                args.test
            ), "For running flaky or bugfix_validation check locally, test case name must be provided via --test"
        else:
            if is_bugfix_validation and Labels.PR_BUGFIX not in info.pr_labels:
                # Not a bugfix PR - run a simple sanity test
                tests = ["00001_select_1"]
            else:
                tests = targeter.get_changed_tests()

        if tests:
            print(f"Test list: [{tests}]")
        else:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED, info="No tests to run"
            ).complete_job()

    if is_targeted_check:
        assert not args.test, "--test not supposed to be used for targeted check ???"
        tests, results_with_info = targeter.get_all_relevant_tests_with_info(ch_path)
        results.append(results_with_info)
        if not tests:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No failed tests found from previous runs",
            ).complete_job()

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    Utils.add_to_PATH(f"{ch_path}:tests")
    CH = ClickHouseProc(
        is_db_replicated=is_database_replicated, is_shared_catalog=is_shared_catalog
    )

    job_info = ""

    if res and JobStages.INSTALL_CLICKHOUSE in stages:

        def configure_log_export():
            if not info.is_local_run:
                print("prepare log export config")
                return CH.create_log_export_config()
            else:
                print("skip log export config for local run")

        commands = [
            f"rm -rf /etc/clickhouse-client/* /etc/clickhouse-server/* /etc/clickhouse-server1/* /etc/clickhouse-server2/*",
            # google *.proto files
            f"mkdir -p /usr/share/clickhouse/ && ln -sf /usr/local/include /usr/share/clickhouse/protos",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-compressor",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-disks",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-obfuscator",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-format",
            f"ln -sf {ch_path}/clickhouse {ch_path}/ch",
            f"ln -sf /usr/bin/clickhouse-odbc-bridge {ch_path}/clickhouse-odbc-bridge",
            f"cp programs/server/config.xml programs/server/users.xml /etc/clickhouse-server/",
            f"./tests/config/install.sh /etc/clickhouse-server /etc/clickhouse-client {config_installs_args}",
            f"clickhouse-server --version",
            f"sed -i 's|>/test/chroot|>{temp_dir}/chroot|' /etc/clickhouse-server**/config.d/*.xml",
            CH.set_random_timezone,
        ]

        if is_flaky_check or is_targeted_check:
            commands.append(CH.enable_thread_fuzzer_config)

        os.environ["MALLOC_CONF"] = (
            f"prof_prefix:{temp_dir}/jemalloc_profiles/clickhouse.jemalloc"
        )

        if not is_coverage:
            commands.append(configure_log_export)

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found under [{ch_path}]"

    if res and JobStages.START in stages:
        step_name = "Start ClickHouse Server"
        print(step_name)

        def start():
            res = CH.start_minio(test_type="stateless") and CH.start_azurite()
            res = res and CH.start()
            res = res and CH.wait_ready()
            if res:
                if not CH.start_kafka():
                    print("WARNING: Failed to start Kafka")

                if not Info().is_local_run:
                    if not CH.start_log_exports(stop_watch.start_time):
                        info.add_workflow_report_message(
                            "WARNING: Failed to start log export"
                        )
                        print("Failed to start log export")
                if not CH.create_minio_log_tables():
                    info.add_workflow_report_message(
                        "WARNING: Failed to create minio log tables"
                    )
                    print("Failed to create minio log tables")

                if has_stateful_tests:
                    res = (
                        CH.prepare_stateful_data(
                            with_s3_storage=is_s3_storage,
                            is_db_replicated=is_database_replicated,
                        )
                        and CH.insert_system_zookeeper_config()
                    )
            if res:
                print("stateful data prepared")
            return res

        results.append(
            Result.from_commands_run(
                name=step_name,
                command=start,
            )
        )
        res = results[-1].is_ok()

    test_result = None
    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)

        # FIXME: Determine optimal mode for targeted job:
        # Mode (1): Run all tests N times in one go (flaky-mode)
        #   - Drawback: Noisy errors when tests can't run in parallel with themselves
        #   - Skips tests marked as no-flaky-check
        # Mode (2): N consequent runs for chosen tests
        #   - Drawback: Might eliminate mode (1) issues but potentially catches fewer problems
        #
        # Mode (1):
        # run_sets_cnt = 1
        # Mode (2):
        run_sets_cnt = rerun_count if is_targeted_check else 1
        rerun_count = 1 if is_targeted_check else rerun_count

        ft_res_processor = FTResultsProcessor(wd=temp_dir)

        # For flaky check, set a soft time limit so that the test runner stops
        # gracefully before the job hard timeout, allowing results to be posted.
        # The job timeout is 2.5 hours (9000s); leave a 1-hour margin for cleanup
        # (server shutdown, system table export, log collection — all slow under sanitizers).
        job_timeout = int(3600 * 2.5)
        soft_limit_margin = 3600
        global_time_limit = 0
        if is_flaky_check or is_targeted_check:
            global_time_limit = max(
                job_timeout - soft_limit_margin - int(stop_watch.duration), 0
            )
            print(
                f"Soft time limit for test runner: {global_time_limit}s"
                f" (elapsed so far: {int(stop_watch.duration)}s)"
            )

        # Track collected test results across multiple runs (only used when run_sets_cnt > 1)
        collected_test_results = []
        seen_test_names = set()
        # Track accumulated run time per test for the targeted check per-test time cap
        test_time_accumulated: dict[str, float] = {}
        TIME_CAP_PER_TEST_SEC = 10 * 60
        tests_to_run = list(tests) if tests else tests

        for cnt in range(run_sets_cnt):
            # For targeted checks with multiple iterations, recalculate
            # the remaining time for each invocation of the test runner.
            if global_time_limit > 0 and run_sets_cnt > 1:
                global_time_limit = max(
                    job_timeout - soft_limit_margin - int(stop_watch.duration), 0
                )

            run_tests(
                batch_num=batch_num if not tests_to_run else 0,
                batch_total=total_batches if not tests_to_run else 0,
                tests=tests_to_run,
                extra_args=runner_options,
                random_order=is_flaky_check
                or is_targeted_check
                or is_bugfix_validation,
                rerun_count=rerun_count,
                global_time_limit=global_time_limit,
            )
            test_result = ft_res_processor.run()

            # Experimental mode for targeted check: collect first failure of each test,
            # or all results on the final attempt
            if run_sets_cnt > 1:
                is_final_run = cnt == run_sets_cnt - 1

                # Accumulate per-test run time and filter tests that exceeded the time cap
                if is_targeted_check:
                    for test_case_result in test_result.results:
                        if test_case_result.duration is not None:
                            test_time_accumulated[test_case_result.name] = (
                                test_time_accumulated.get(test_case_result.name, 0.0)
                                + test_case_result.duration
                            )
                    if not is_final_run:
                        tests_to_run = [
                            t
                            for t in tests_to_run
                            if test_time_accumulated.get(t, 0.0)
                            < TIME_CAP_PER_TEST_SEC
                        ]
                        if not tests_to_run:
                            print(
                                "NOTE: All tests exceeded the time cap; stopping early"
                            )
                            is_final_run = True

                for test_case_result in test_result.results:
                    # Only collect each test once (first failure or final result)
                    if test_case_result.name not in seen_test_names:
                        # On non-final runs: collect only failed test cases
                        # On final run: collect all remaining test cases
                        should_collect = not test_case_result.is_ok() or is_final_run
                        if should_collect:
                            test_case_result.set_info(
                                f"Run attempt {cnt + 1} out of {run_sets_cnt}"
                            )
                            collected_test_results.append(test_case_result)
                            seen_test_names.add(test_case_result.name)

                stop_by_elapsed_time = global_time_limit <= 0

                # On final run, replace results with collected ones
                if is_final_run or stop_by_elapsed_time:
                    test_result.results = collected_test_results
                    # Set overall status to failed if any collected test cases failed
                    has_failures = any(not t.is_ok() for t in collected_test_results)
                    if has_failures and test_result.is_ok():
                        test_result.set_failed()
                    break

        if not info.is_local_run:
            CH.stop_log_exports()

        results.append(test_result)
        debug_files += ft_res_processor.debug_files

        results[-1].set_timing(stopwatch=stop_watch_)
        if results[-1].info:
            job_info = results[-1].info
            results[-1].info = ""

        res = results[-1].is_ok()

    if JobStages.RETRIES in stages and test_result and test_result.is_failure():
        # retry all failed tests and mark original failed either as success on retry or failed on retry
        failed_tests = []
        for t in test_result.results:
            if t.is_failure() and t.name and t.name[0].isdigit():
                failed_tests.append(t.name)
            elif t.is_error():
                failed_tests = []
                print(
                    "NOTE: Skipping retry stage because the main test run ended with errors"
                )
                break

        if len(failed_tests) > 10:
            results.append(
                Result(
                    name="Retries",
                    status=Result.Status.SKIPPED,
                    info="Too many failed tests",
                )
            )
        elif failed_tests:
            ft_res_processor = FTResultsProcessor(wd=temp_dir)
            run_tests(
                batch_num=0,
                batch_total=0,
                tests=failed_tests,
                extra_args=runner_options,
                random_order=True,
                rerun_count=1,
            )
            retry_result = ft_res_processor.run(task_name="Retries")
            if retry_result.is_failure():
                # do not produce noise failures
                retry_result.set_success()
            success_after_rerun = [t.name for t in retry_result.results if t.is_ok()]
            failed_after_rerun = [
                t.name for t in retry_result.results if t.is_failure()
            ]
            if success_after_rerun or failed_after_rerun:
                for test_case in test_result.results:
                    if test_case.name in success_after_rerun:
                        if is_llvm_coverage:
                            print(
                                f"Test {test_case.name} has succeeded after rerun. Mark it as OK"
                            )
                            test_case.remove_label(Result.Status.FAILED)
                            test_case.remove_label(Result.StatusExtended.FAIL)
                            test_case.set_status(Result.StatusExtended.OK)
                        else:
                            test_case.set_label(Result.Label.OK_ON_RETRY)
                    elif test_case.name in failed_after_rerun:
                        test_case.set_label(Result.Label.FAILED_ON_RETRY)
            results.append(retry_result)

    if args.debug:
        print("\n\n=== Debug mode enabled, starting clickhouse-client ===\n")
        subprocess.call("clickhouse-client", shell=True)

    CH.terminate()

    reset_success = False
    if (
        test_result
        and not test_result.is_error()
        and JobStages.COLLECT_COVERAGE in stages
    ):
        print("Collect coverage")
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        results.append(
            Result.from_commands_run(
                name="Collect coverage",
                command=lambda: CoverageExporter(
                    src=CH,
                    dest=cidb_cluster,
                    job_name=info.job_name,
                ).do(),
            )
        )
        if results[-1].is_ok():
            reset_success = True

    if test_result and JobStages.CHECK_ERRORS in stages:
        # must not be performed for a test validation - test must fail and log errors are not respected
        print("Check fatal errors")
        sw_ = Utils.Stopwatch()
        results.append(
            Result.create_from(
                name="Check errors",
                results=CH.check_fatal_messages_in_logs(),
                status=Result.Status.SUCCESS,
                stopwatch=sw_,
            )
        )
        # fatal failures found in logs represented as normal test cases
        test_result.extend_sub_results(results[-1].results)
        results[-1].results = []

    # invert result status for bugfix validation
    if is_bugfix_validation and test_result and Labels.PR_BUGFIX in info.pr_labels:
        has_failure = False
        for r in test_result.results:
            r.set_label("xfail")
            if r.status == Result.StatusExtended.FAIL:
                r.status = Result.StatusExtended.OK
                has_failure = True
            elif r.status == Result.StatusExtended.OK:
                r.status = Result.StatusExtended.FAIL
        if not has_failure:
            print("Failed to reproduce the bug")
            test_result.set_failed().set_info("Failed to reproduce the bug")
        else:
            # For bugfix validation, the expected behavior is:
            # - At least one test must fail (bug reproduced)
            # - The overall Tests result is treated as success in that case
            test_result.set_success()

        # For bugfix validation, "Check errors" (latest in the list) is only a helper step and
        # must not affect the overall job result.
        results[-1].set_success()

    if JobStages.COLLECT_LOGS in stages:
        print("Collect logs")

        def collect_logs():
            CH.prepare_logs(all=test_result and not test_result.is_ok(), info=info)

        results.append(
            Result.from_commands_run(
                name="Collect logs",
                command=collect_logs,
            )
        )
        if test_result and CH.extra_tests_results:
            test_result.extend_sub_results(CH.extra_tests_results)

    # Decide whether to block the CI pipeline on test failures
    force_ok_exit = False
    if "parallel" in test_options and test_result:
        failures_cnt = len([r for r in test_result.results if not r.is_ok()])
        if failures_cnt > 0 and failures_cnt < 4:
            print(
                f"NOTE: Failed {failures_cnt} tests - do not block pipeline, exit with 0"
            )
            force_ok_exit = True
        elif failures_cnt > 0 and "ci-non-blocking" in info.pr_labels:
            print(
                f"NOTE: Failed {failures_cnt} tests, label 'ci-non-blocking' is set - do not block pipeline - exit with 0"
            )
            force_ok_exit = True
    if is_llvm_coverage and test_result:
        # do not block pipeline on amd_llvm_coverage job failures
        print("NOTE: LLVM coverage job - do not block pipeline - exit with 0")
        force_ok_exit = True

    if test_result:
        test_result.sort()

    R = Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=CH.logs + debug_files,
        info=job_info,
    )

    if is_llvm_coverage:
        print("Collecting and merging LLVM coverage files...")
        Shell.get_output("pwd", verbose=True).strip().split("\n")
        profraw_files = (
            Shell.get_output("find . -name '*.profraw'", verbose=True)
            .strip()
            .split("\n")
        )
        profraw_files = [f.strip() for f in profraw_files if f.strip()]

        if profraw_files:
            print(f"Found {len(profraw_files)} .profraw files:")
            for f in profraw_files:
                try:
                    size_bytes = os.path.getsize(f)
                    print(f"  {size_bytes:>12} bytes | {f}")
                except OSError:
                    continue

            # Auto-detect available LLVM profdata tool
            llvm_profdata = None
            for ver in ["21", "20", "18", "19", "17", "16", ""]:
                cmd = f"llvm-profdata{'-' + ver if ver else ''}"
                if Shell.check(f"command -v {cmd}", verbose=False):
                    llvm_profdata = cmd
                    break

            if not llvm_profdata:
                print("ERROR: llvm-profdata not found in PATH")
            else:
                print(f"Using {llvm_profdata} to merge coverage files")

                # Merge all profraw files to current directory
                merged_file = f"./ft-{batch_num}.profdata"
                merge_cmd = f"{llvm_profdata} merge -sparse -failure-mode=warn {' '.join(profraw_files)} -o {merged_file} 2>&1"
                merge_output = Shell.get_output(merge_cmd, verbose=True)

                # Check for corrupted files in the output
                corrupted_files = [
                    line
                    for line in merge_output.split("\n")
                    if "invalid instrumentation profile" in line
                    or "file header is corrupt" in line
                ]
                if corrupted_files:
                    print(
                        f"WARNING: Found {len(corrupted_files)} corrupted profraw files:"
                    )
                    for corrupted in corrupted_files:
                        print(f"  {corrupted}")

        else:
            print("No .profraw files found for coverage")

    if reset_success:
        # coverage job ignores test failures
        R.set_success()

    R.complete_job(do_not_block_pipeline_on_failure=force_ok_exit)


if __name__ == "__main__":
    main()
