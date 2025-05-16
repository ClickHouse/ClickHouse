import argparse
import os
import re
import time
from pathlib import Path

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("--ch-path", help="Path to clickhouse binary", default=temp_dir)
    parser.add_argument(
        "--options",
        help="Comma separated option(s): parallel|non-parallel|BATCH_NUM/BTATCH_TOT|..",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def get_changed_tests(info: Info):
    result = set()
    changed_files = info.get_changed_files()
    assert changed_files, "No changed files"

    for fpath in changed_files:
        if re.match(r"tests/queries/0_stateless/\d{5}", fpath):
            if not Path(fpath).exists():
                print(f"File '{fpath}' was removed — skipping")
                continue

            print(f"Detected changed test file: '{fpath}'")

            fname = os.path.basename(fpath)
            fname_without_ext = os.path.splitext(fname)[0]

            # Add '.' suffix to precisely match this test only
            result.add(f"{fname_without_ext}.")

        elif fpath.startswith("tests/queries/"):
            # Log any other suspicious file in tests/queries for future debugging
            print(f"File '{fpath}' changed, but doesn't match expected test pattern")

    return sorted(result)


def run_tests(
    no_parallel: bool,
    no_sequiential: bool,
    batch_num: int,
    batch_total: int,
    test="",
    extra_args="",
):
    assert not (no_parallel and no_sequiential)
    test_output_file = f"{temp_dir}/test_result.txt"
    nproc = int(Utils.cpu_count() / 2)
    if batch_num and batch_total:
        extra_args += (
            f" --run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
        )
    else:
        extra_args += " --report-coverage"
    if "--no-shard" not in extra_args:
        extra_args += " --shard"
    if "--no-zookeeper" not in extra_args:
        extra_args += " --zookeeper"
    command = f"clickhouse-test --testname --check-zookeeper-session --hung-check \
                --capture-client-stacktrace --queries ./tests/queries --test-runs 1 --hung-check \
                {'--no-parallel' if no_parallel else ''}  {'--no-sequential' if no_sequiential else ''} \
                --jobs {nproc} --report-logs-stats {extra_args} \
                --queries ./tests/queries -- '{test}' | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(command, verbose=True)


def run_specific_tests(tests, runs=1):
    test_output_file = f"{temp_dir}/test_result.txt"
    nproc = int(Utils.cpu_count() / 2)
    command = f"clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check \
        --capture-client-stacktrace --queries ./tests/queries --report-logs-stats --test-runs {runs} \
        --jobs {nproc} --order=random -- {' '.join(tests)} | ts '%Y-%m-%d %H:%M:%S' | tee -a \"{test_output_file}\""
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
    "DatabaseReplicated": " --no-stateful --replicated-database --jobs 3",
}


def main():
    args = parse_args()
    test_options = [to.strip() for to in args.options.split(",")]
    no_parallel = "non-parallel" in test_options
    no_sequential = "parallel" in test_options
    batch_num, total_batches = 0, 0
    config_installs_args = ""
    is_flaky_check = False
    is_bugfix_validation = False
    is_s3_storage = False
    is_database_replicated = False
    runner_options = ""
    info = Info()

    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif to in OPTIONS_TO_INSTALL_ARGUMENTS:
            print(f"NOTE: Enabled config option [{OPTIONS_TO_INSTALL_ARGUMENTS[to]}]")
            config_installs_args += f" {OPTIONS_TO_INSTALL_ARGUMENTS[to]}"
        else:
            if to.startswith("amd_") or to.startswith("arm_") or "flaky" in to:
                pass
            else:
                assert False, f"Unknown option [{to}]"

        if to in OPTIONS_TO_TEST_RUNNER_ARGUMENTS:
            runner_options += f" {OPTIONS_TO_TEST_RUNNER_ARGUMENTS[to]}"

        if "flaky" in to:
            is_flaky_check = True
        elif "BugfixValidation" in to:
            is_bugfix_validation = True

        if "s3 storage" in to:
            is_s3_storage = True
        if "DatabaseReplicated" in to:
            is_database_replicated = True

    # TODO: find a way to work with Azure secret so it's ok for local tests as well, for now keep azure disabled
    if not info.is_local_run:
        os.environ["AZURE_CONNECTION_STRING"] = Shell.get_output(
            f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
            verbose=True,
        )

    ch_path = args.ch_path

    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)

    logs_to_attach = []

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    results = []

    Utils.add_to_PATH(f"{ch_path}:tests")
    CH = ClickHouseProc()

    if res and JobStages.INSTALL_CLICKHOUSE in stages:

        def configure_log_export():
            if not info.is_local_run:
                print("prepare log export config")
                return CH.create_log_export_config()
            else:
                print("skip log export config for local run")

        commands = [
            f"rm -rf {temp_dir}/var/log/clickhouse-server/clickhouse-server.*",
            f"chmod +x {ch_path}/clickhouse",
            # google *.proto files
            f"mkdir -p /usr/share/clickhouse/ && ln -sf /usr/local/include /usr/share/clickhouse/protos",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-compressor",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-disks",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-obfuscator",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-format",
            f"ln -sf /usr/bin/clickhouse-odbc-bridge {ch_path}/clickhouse-odbc-bridge",
            f"rm -rf {temp_dir}/etc/ && mkdir -p {temp_dir}/etc/clickhouse-client {temp_dir}/etc/clickhouse-server",
            f"cp programs/server/config.xml programs/server/users.xml {temp_dir}/etc/clickhouse-server/",
            f"./tests/config/install.sh {temp_dir}/etc/clickhouse-server {temp_dir}/etc/clickhouse-client {config_installs_args}",
            # clickhouse benchmark segfaults with --config-path, so provide client config by its default location
            f"mkdir -p /etc/clickhouse-client && cp {temp_dir}/etc/clickhouse-client/* /etc/clickhouse-client/",
            # update_path_ch_config,
            # f"sed -i 's|>/var/|>{temp_dir}/var/|g; s|>/etc/|>{temp_dir}/etc/|g' {temp_dir}/etc/clickhouse-server/config.xml",
            # f"sed -i 's|>/etc/|>{temp_dir}/etc/|g' {temp_dir}/etc/clickhouse-server/config.d/ssl_certs.xml",
            f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g' $(readlink -f $file); done",
            f"for file in {temp_dir}/etc/clickhouse-server/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g' $(readlink -f $file); done",
            f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|<path>local_disk|<path>{temp_dir}/local_disk|g' $(readlink -f $file); done",
            f"clickhouse-server --version",
        ]

        # sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server1/config.xml --daemon \
        #                                           --pid-file /var/run/clickhouse-server1/clickhouse-server.pid \
        #                                           -- --path /var/lib/clickhouse1/ --logger.stderr /var/log/clickhouse-server/stderr1.log \
        #                                           --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log \
        #                                           --tcp_port 19000 --tcp_port_secure 19440 --http_port 18123 --https_port 18443 --interserver_http_port 19009 --tcp_with_proxy_port 19010 \
        #
        # sudo -E -u clickhouse /usr/bin/clickhouse server --config /etc/clickhouse-server2/config.xml --daemon \
        #                                           --pid-file /var/run/clickhouse-server2/clickhouse-server.pid \
        #                                           -- --path /var/lib/clickhouse2/ --logger.stderr /var/log/clickhouse-server/stderr2.log \
        #                                           --logger.log /var/log/clickhouse-server/clickhouse-server2.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server2.err.log \
        #                                           --tcp_port 29000 --tcp_port_secure 29440 --http_port 28123 --https_port 28443 --interserver_http_port 29009 --tcp_with_proxy_port 29010 \

        if is_flaky_check:
            commands.append(CH.enable_thread_fuzzer_config)
        elif is_bugfix_validation:
            if Utils.is_arm():
                link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
            else:
                link_to_master_head_binary = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
            if not info.is_local_run or not (Path(temp_dir) / "clickhouse").exists():
                print(
                    f"NOTE: Clickhouse binary will be downloaded to [{temp_dir}] from [{link_to_master_head_binary}]"
                )
                if info.is_local_run:
                    time.sleep(10)
                commands.insert(
                    0,
                    f"wget -nv -P {temp_dir} {link_to_master_head_binary}",
                )
            os.environ["GLOBAL_TAGS"] = "no-random-settings"

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
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        minio_log = f"{temp_dir}/minio.log"
        azurite_log = f"{temp_dir}/azurite.log"
        res = (
            res
            and CH.start_minio(test_type="stateless", log_file_path=minio_log)
            and CH.start_azurite(log_file_path=azurite_log)
        )
        logs_to_attach += [minio_log, azurite_log]
        time.sleep(10)
        Shell.check("ps -ef | grep minio", verbose=True)
        res = res and Shell.check(
            "aws s3 ls s3://test --endpoint-url http://localhost:11111/", verbose=True
        )
        res = res and CH.start(replicated=is_database_replicated)
        res = res and CH.wait_ready()
        if not Info().is_local_run:
            if not CH.start_log_exports(stop_watch.start_time):
                info.add_workflow_report_message("WARNING: Failed to start log export")
                print("Failed to start log export")
        if res:
            print("ch started")
        logs_to_attach += [
            f"{temp_dir}/var/log/clickhouse-server/clickhouse-server.log",
            f"{temp_dir}/var/log/clickhouse-server/clickhouse-server.err.log",
        ]
        res = (
            res
            and CH.prepare_stateful_data(
                with_s3_storage=is_s3_storage, is_db_replicated=is_database_replicated
            )
            and CH.insert_system_zookeeper_config()
        )
        if res:
            print("stateful data prepared")
        results.append(
            Result.create_from(
                name=step_name,
                status=res,
                stopwatch=stop_watch_,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)
        if not is_flaky_check and not is_bugfix_validation:
            run_tests(
                no_parallel=no_parallel,
                no_sequiential=no_sequential,
                batch_num=batch_num,
                batch_total=total_batches,
                test=args.test,
                extra_args=runner_options,
            )
        else:
            # Flaky or Bugfix Validation check
            if info.is_local_run:
                assert (
                    args.test
                ), "For running flaky or bugfix_validation check locally, test case name must be provided via --test"
                tests = [args.test]
            else:
                tests = get_changed_tests(info)
            if tests:
                run_specific_tests(tests=tests, runs=50 if is_flaky_check else 1)
            else:
                print("WARNING: No tests to run")
        CH.stop_log_exports()
        results.append(FTResultsProcessor(wd=temp_dir).run())

        # invert result status for bugfix validation
        if is_bugfix_validation:
            has_failure = False
            for r in results[-1].results:
                if not r.is_ok():
                    has_failure = True
                    break
            if not has_failure:
                print("Failed to reproduce the bug")
                results[-1].set_failed()
            else:
                results[-1].set_success()

        results[-1].set_timing(stopwatch=stop_watch_)
        res = results[-1].is_ok()

    # TODO: collect logs
    # and CH.flush_system_logs()

    Result.create_from(
        results=results, stopwatch=stop_watch, files=logs_to_attach if not res else []
    ).complete_job()


if __name__ == "__main__":
    main()
