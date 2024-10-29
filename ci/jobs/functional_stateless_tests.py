import argparse
import os
from pathlib import Path

from praktika.param import get_param
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.settings.definitions import azure_secret


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument(
        "BUILD_TYPE", help="Type: <amd|arm>_<debug|release>_<asan|tsan|..>"
    )
    parser.add_argument("--param", help="Optional custom job start stage", default=None)
    return parser.parse_args()


def run_stateless_test(
    no_parallel: bool, no_sequiential: bool, batch_num: int, batch_total: int
):
    assert not (no_parallel and no_sequiential)
    test_output_file = f"{Settings.OUTPUT_DIR}/test_result.txt"
    aux = ""
    nproc = int(Utils.cpu_count() / 2)
    if batch_num and batch_total:
        aux = f"--run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
    statless_test_command = f"clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check --print-time \
                --no-drop-if-fail --capture-client-stacktrace --queries /repo/tests/queries --test-runs 1 --hung-check \
                {'--no-parallel' if no_parallel else ''}  {'--no-sequential' if no_sequiential else ''} \
                --print-time --jobs {nproc} --report-coverage --report-logs-stats {aux} \
                --queries ./tests/queries -- '' | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(statless_test_command, verbose=True)


def main():

    args = parse_args()
    params = get_param().split(" ")
    parallel_or_sequential = None
    no_parallel = False
    no_sequential = False
    if params:
        parallel_or_sequential = params[0]
    if len(params) > 1:
        batch_num, total_batches = map(int, params[1].split("/"))
    else:
        batch_num, total_batches = 0, 0
    if parallel_or_sequential:
        no_parallel = parallel_or_sequential == "non-parallel"
        no_sequential = parallel_or_sequential == "parallel"

    os.environ["AZURE_CONNECTION_STRING"] = Shell.get_output(
        f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
        verbose=True,
    )

    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    results = []

    Utils.add_to_PATH(f"{Settings.INPUT_DIR}:tests")

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        commands = [
            f"chmod +x {Settings.INPUT_DIR}/clickhouse",
            f"ln -sf {Settings.INPUT_DIR}/clickhouse {Settings.INPUT_DIR}/clickhouse-server",
            f"ln -sf {Settings.INPUT_DIR}/clickhouse {Settings.INPUT_DIR}/clickhouse-client",
            f"rm -rf {Settings.TEMP_DIR}/etc/ && mkdir -p {Settings.TEMP_DIR}/etc/clickhouse-client {Settings.TEMP_DIR}/etc/clickhouse-server",
            f"cp programs/server/config.xml programs/server/users.xml {Settings.TEMP_DIR}/etc/clickhouse-server/",
            f"./tests/config/install.sh {Settings.TEMP_DIR}/etc/clickhouse-server {Settings.TEMP_DIR}/etc/clickhouse-client --s3-storage",
            # update_path_ch_config,
            f"sed -i 's|>/var/|>{Settings.TEMP_DIR}/var/|g; s|>/etc/|>{Settings.TEMP_DIR}/etc/|g' {Settings.TEMP_DIR}/etc/clickhouse-server/config.xml",
            f"sed -i 's|>/etc/|>{Settings.TEMP_DIR}/etc/|g' {Settings.TEMP_DIR}/etc/clickhouse-server/config.d/ssl_certs.xml",
            f"clickhouse-server --version",
        ]
        results.append(
            Result.create_from_command_execution(
                name="Install ClickHouse", command=commands, with_log=True
            )
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc()
    if res and JobStages.START in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        res = res and CH.start_minio()
        res = res and CH.start()
        res = res and CH.wait_ready()
        results.append(
            Result.create_from(
                name=step_name,
                status=res,
                stopwatch=stop_watch_,
                files=(
                    [
                        "/tmp/praktika/var/log/clickhouse-server/clickhouse-server.log",
                        "/tmp/praktika/var/log/clickhouse-server/clickhouse-server.err.log",
                    ]
                    if not res
                    else []
                ),
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)
        run_stateless_test(
            no_parallel=no_parallel,
            no_sequiential=no_sequential,
            batch_num=batch_num,
            batch_total=total_batches,
        )
        results.append(FTResultsProcessor(wd=Settings.OUTPUT_DIR).run())
        results[-1].set_timing(stopwatch=stop_watch_)
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
