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
    no_parallel: bool, no_sequiential: bool, batch_num: int, batch_total: int, test=""
):
    assert not (no_parallel and no_sequiential)
    test_output_file = f"{temp_dir}/test_result.txt"
    aux = ""
    nproc = int(Utils.cpu_count() / 2)
    if batch_num and batch_total:
        aux = f"--run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
    command = f"clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check \
                --capture-client-stacktrace --queries /repo/tests/queries --test-runs 1 --hung-check \
                {'--no-parallel' if no_parallel else ''}  {'--no-sequential' if no_sequiential else ''} \
                --jobs {nproc} --report-coverage --report-logs-stats {aux} \
                --queries ./tests/queries -- '{test}' | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(command, verbose=True)


def run_tests_flaky_check(tests):
    test_output_file = f"{temp_dir}/test_result.txt"
    nproc = int(Utils.cpu_count() / 2)
    command = f"clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check \
        --capture-client-stacktrace --queries /repo/tests/queries --report-logs-stats --test-runs 50 \
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
}


def main():
    args = parse_args()
    test_options = [to.strip() for to in args.options.split(",")]
    no_parallel = "non-parallel" in test_options
    no_sequential = "parallel" in test_options
    batch_num, total_batches = 0, 0
    config_installs_args = ""
    is_flaky_check = False
    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif to in OPTIONS_TO_INSTALL_ARGUMENTS:
            print(f"NOTE: Enabled config option [{OPTIONS_TO_INSTALL_ARGUMENTS[to]}]")
            config_installs_args += f" {OPTIONS_TO_INSTALL_ARGUMENTS[to]}"
        elif "flaky" in to:
            is_flaky_check = True
        else:
            if to.startswith("amd_") or to.startswith("arm_"):
                # this is a binary type
                continue
            assert False, f"Unknown option [{to}]"

    # TODO: find a way to work with Azure secret so it's ok for local tests as well, for now keep azure disabled
    info = Info()
    if not info.is_local_run:
        os.environ["AZURE_CONNECTION_STRING"] = Shell.get_output(
            f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
            verbose=True,
        )

    ch_path = args.ch_path
    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found under [{ch_path}]"

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
            configure_log_export,
        ]
        if is_flaky_check:
            commands.append(CH.enable_thread_fuzzer_config)
            # FIXME:
            # if [ "$NUM_TRIES" -gt "1" ]; then
            #     # We don't run tests with Ordinary database in PRs, only in master.
            #     # So run new/changed tests with Ordinary at least once in flaky check.
            #     NUM_TRIES=1 USE_DATABASE_ORDINARY=1 run_tests \
            #                                         | sed 's/All tests have finished/Redacted: a message about tests finish is deleted/' | sed 's/No tests were run/Redacted: a message about no tests run is deleted/' ||:
            # fi

        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

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
        res = res and CH.start()
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
        res = res and CH.prepare_stateful_data()
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
        assert Shell.check(
            "clickhouse-client -q \"insert into system.zookeeper (name, path, value) values ('auxiliary_zookeeper2', '/test/chroot/', '')\"",
            verbose=True,
        )
        if not is_flaky_check:
            run_tests(
                no_parallel=no_parallel,
                no_sequiential=no_sequential,
                batch_num=batch_num,
                batch_total=total_batches,
                test=args.test,
            )
        else:
            if info.is_local_run:
                assert (
                    args.test
                ), "For runnin flaky check localy test case name must be provided via --test"
                tests = [args.test]
            else:
                tests = get_changed_tests(info)
            if tests:
                run_tests_flaky_check(tests=tests)
            else:
                print("WARNING: No tests to run")
        CH.stop_log_exports()
        results.append(FTResultsProcessor(wd=temp_dir).run())
        results[-1].set_timing(stopwatch=stop_watch_)
        res = results[-1].is_ok()

    Result.create_from(
        results=results, stopwatch=stop_watch, files=logs_to_attach if not res else []
    ).complete_job()


if __name__ == "__main__":
    main()
