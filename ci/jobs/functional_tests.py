import argparse
import os
import random
import re
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.functional_tests.export_coverage import CoverageExporter
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"
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
    return parser.parse_args()


def run_tests(
    batch_num: int,
    batch_total: int,
    tests: list[str] = None,
    extra_args="",
    rerun_count=1,
    random_order=False,
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
    command = f"clickhouse-test --testname --check-zookeeper-session --hung-check --trace \
                --capture-client-stacktrace --queries ./tests/queries --test-runs {rerun_count} \
                {extra_args} \
                --queries ./tests/queries {('--order=random' if random_order else '')} -- {' '.join(tests) if tests else ''} | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(command, verbose=True)


OPTIONS_TO_INSTALL_ARGUMENTS = {
    "old analyzer": "--analyzer",
    "s3 storage": "--s3-storage",
    "DatabaseReplicated": "--db-replicated",
    "DatabaseOrdinary": "--db-ordinary",
    "wide parts enabled": "--wide-parts",
    "ParallelReplicas": "--parallel-rep",
    "distributed plan": "--distributed-plan",
    "azure": "--azure",
    "AsyncInsert": " --async-insert",
    "BugfixValidation": " --bugfix-validation",
}

OPTIONS_TO_TEST_RUNNER_ARGUMENTS = {
    "s3 storage": "--s3-storage --no-stateful",
    "ParallelReplicas": "--no-zookeeper --no-shard --no-parallel-replicas",
    "AsyncInsert": " --no-async-insert",
    "DatabaseReplicated": " --no-stateful --replicated-database",
    "azure": " --azure-blob-storage --no-random-settings --no-random-merge-tree-settings",  # azurite is slow, with randomization it can be super slow
    "parallel": "--no-sequential",
    "sequential": "--no-parallel",
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
    is_coverage = False
    runner_options = ""
    # optimal value for most of the jobs
    nproc = int(Utils.cpu_count() * 0.6)
    info = Info()

    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif to in OPTIONS_TO_INSTALL_ARGUMENTS:
            print(f"NOTE: Enabled config option [{OPTIONS_TO_INSTALL_ARGUMENTS[to]}]")
            config_installs_args += f" {OPTIONS_TO_INSTALL_ARGUMENTS[to]}"
        elif (
            to.startswith("amd_")
            or to.startswith("arm_")
            or "flaky" in to
            or "targeted" in to
        ):
            pass
        elif to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            print(
                f"NOTE: Enabled test runner option [{OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}]"
            )
        else:
            assert False, f"Unknown option [{to}]"

        if to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            if to in ("parallel", "sequential") and args.test:
                # skip setting up parallel/sequential if specific tests are provided
                continue
            runner_options += f" {OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}"

        if "targeted" in to:
            is_targeted_check = True
        elif "flaky" in to:
            is_flaky_check = True
        elif "BugfixValidation" in to:
            is_bugfix_validation = True
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
        elif is_coverage:
            cidb_cluster = CIDBCluster()
            assert cidb_cluster.is_ready()
            nproc = 1
        else:
            pass

    if args.workers:
        print(f"Workers count set from --workers: {args.workers}")
        runner_options += f" --jobs {args.workers}"
    else:
        print(f"Workers count set to optimal value: {nproc}")
        runner_options += f" --jobs {nproc}"

    rerun_count = 1
    if args.count:
        print(f"Rerun count set from --count: {args.count}")
        rerun_count = args.count
    elif is_flaky_check:
        print(f"Rerun count set to 50 for flaky check")
        rerun_count = 50
    elif is_targeted_check:
        print(f"Rerun count set to 5 for targeted check")
        rerun_count = 10

    if not info.is_local_run:
        # TODO: find a way to work with Azure secret so it's ok for local tests as well, for now keep azure disabled
        os.environ["AZURE_CONNECTION_STRING"] = Shell.get_output(
            f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
            verbose=True,
        )
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
    if is_coverage or is_bugfix_validation or info.is_local_run:
        # not needed for these job flavors
        # moreover it updates/rewrites inverted Tests status for bugfix validation check which should stay inverted
        stages.remove(JobStages.CHECK_ERRORS)
    if info.is_local_run:
        if JobStages.COLLECT_LOGS in stages:
            stages.remove(JobStages.COLLECT_LOGS)
        if JobStages.COLLECT_COVERAGE in stages:
            stages.remove(JobStages.COLLECT_COVERAGE)

    tests = args.test
    targeter = Targeting(info=info)
    if is_flaky_check or is_bugfix_validation:
        if info.is_local_run:
            assert (
                args.test
            ), "For running flaky or bugfix_validation check locally, test case name must be provided via --test"
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
            f"rm -rf /etc/clickhouse-client/* /etc/clickhouse-server/*",
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
            f"prof_active:true,prof_prefix:{temp_dir}/jemalloc_profiles/clickhouse.jemalloc"
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

        # Experimental mode for targeted flavor: run a test subset N times instead of repeating each test N times
        #   in a single run to avoid failures for tests that cannot run in parallel with themselves
        run_sets_cnt = 1
        if is_targeted_check:
            run_sets_cnt = rerun_count
            rerun_count = 1
        ft_res_processor = FTResultsProcessor(wd=temp_dir)
        for cnt in range(run_sets_cnt):
            ft_res_processor.debug_files = []
            run_tests(
                batch_num=batch_num if not tests else 0,
                batch_total=total_batches if not tests else 0,
                tests=tests,
                extra_args=runner_options,
                random_order=is_flaky_check
                or is_targeted_check
                or is_bugfix_validation,
                rerun_count=rerun_count,
            )
            test_result = ft_res_processor.run()
            if is_targeted_check:
                test_result.set_info(f"Run attempt {cnt + 1} out of {run_sets_cnt}")
            if not test_result.is_ok():
                break

        if not info.is_local_run:
            CH.stop_log_exports()

        results.append(test_result)
        debug_files += ft_res_processor.debug_files

        # invert result status for bugfix validation
        if is_bugfix_validation:
            has_failure = False
            for r in results[-1].results:
                r.set_label("xfail")
                if r.status == Result.StatusExtended.FAIL:
                    r.status = Result.StatusExtended.OK
                    has_failure = True
                elif r.status == Result.StatusExtended.OK:
                    r.status = Result.StatusExtended.FAIL
            if not has_failure:
                print("Failed to reproduce the bug")
                results[-1].set_failed().set_info("Failed to reproduce the bug")
            else:
                results[-1].set_success()

        results[-1].set_timing(stopwatch=stop_watch_)
        if results[-1].info:
            job_info = results[-1].info
            results[-1].info = ""

        res = results[-1].is_ok()

    CH.terminate()

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
        if not results[-1].is_ok():
            results[-1].set_info("Found errors added into Tests results")

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

    if test_result:
        test_result.sort()

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=CH.logs + debug_files,
        info=job_info,
    ).complete_job(do_not_block_pipeline_on_failure=force_ok_exit)


if __name__ == "__main__":
    main()
