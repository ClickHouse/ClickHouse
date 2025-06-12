import argparse
import os
import time
from pathlib import Path

from praktika.result import Result
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor

temp_dir = f"{Utils.cwd()}/ci/tmp/"


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    START = "start"
    TEST = "test"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("--ch-path", help="Path to clickhouse binary", default=temp_dir)
    parser.add_argument(
        "--test-options",
        help="Comma separated option(s): parallel|non-parallel|BATCH_NUM/BTATCH_TOT|..",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def run_stateless_test(
    no_parallel: bool, no_sequiential: bool, batch_num: int, batch_total: int, test=""
):
    assert not (no_parallel and no_sequiential)
    test_output_file = f"{temp_dir}/test_result.txt"
    aux = ""
    nproc = int(Utils.cpu_count() / 2)
    if batch_num and batch_total:
        aux = f"--run-by-hash-total {batch_total} --run-by-hash-num {batch_num-1}"
    statless_test_command = f"clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check --trace \
                --capture-client-stacktrace --queries /repo/tests/queries --test-runs 1 --hung-check \
                {'--no-parallel' if no_parallel else ''}  {'--no-sequential' if no_sequiential else ''} \
                --jobs {nproc} --report-coverage --report-logs-stats {aux} \
                --queries ./tests/queries -- '{test}' | ts '%Y-%m-%d %H:%M:%S' \
                | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(statless_test_command, verbose=True)


def main():

    args = parse_args()
    test_options = args.test_options.split(",")
    no_parallel = "non-parallel" in test_options
    no_sequential = "parallel" in test_options
    batch_num, total_batches = 0, 0
    for to in test_options:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))

    # TODO: find a way to work with Azure secret so it's ok for local tests as well, for now keep azure disabled
    os.environ["AZURE_CONNECTION_STRING"] = Shell.get_output(
        f"aws ssm get-parameter --region us-east-1 --name azure_connection_string --with-decryption --output text --query Parameter.Value",
        verbose=True,
    )
    no_azure = False
    if not os.environ["AZURE_CONNECTION_STRING"]:
        no_azure = True

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

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        commands = [
            f"rm -rf {temp_dir}/var/log/clickhouse-server/clickhouse-server.*",
            f"chmod +x {ch_path}/clickhouse",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-compressor",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-disks",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-obfuscator",
            f"ln -sf {ch_path}/clickhouse {ch_path}/clickhouse-format",
            f"rm -rf {temp_dir}/etc/ && mkdir -p {temp_dir}/etc/clickhouse-client {temp_dir}/etc/clickhouse-server",
            f"cp programs/server/config.xml programs/server/users.xml {temp_dir}/etc/clickhouse-server/",
            f"./tests/config/install.sh {temp_dir}/etc/clickhouse-server {temp_dir}/etc/clickhouse-client --s3-storage {'--no-azure' if no_azure else ''}",
            # clickhouse benchmark segfaults with --config-path, so provide client config by its default location
            f"cp {temp_dir}/etc/clickhouse-client/* /etc/clickhouse-client/",
            # update_path_ch_config,
            # f"sed -i 's|>/var/|>{temp_dir}/var/|g; s|>/etc/|>{temp_dir}/etc/|g' {temp_dir}/etc/clickhouse-server/config.xml",
            # f"sed -i 's|>/etc/|>{temp_dir}/etc/|g' {temp_dir}/etc/clickhouse-server/config.d/ssl_certs.xml",
            f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g' $(readlink -f $file); done",
            f"for file in {temp_dir}/etc/clickhouse-server/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g' $(readlink -f $file); done",
            f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|<path>local_disk|<path>{temp_dir}/local_disk|g' $(readlink -f $file); done",
            f"clickhouse-server --version",
        ]
        results.append(
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc()
    if res and JobStages.START in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        minio_log = f"{temp_dir}/minio.log"
        res = res and CH.start_minio(test_type="stateless", log_file_path=minio_log)
        logs_to_attach += [minio_log]
        time.sleep(10)
        Shell.check("ps -ef | grep minio", verbose=True)
        res = res and Shell.check(
            "aws s3 ls s3://test --endpoint-url http://localhost:11111/", verbose=True
        )
        res = res and CH.log_cluster_config()
        res = res and CH.start()
        res = res and CH.wait_ready()
        if res:
            print("ch started")
        logs_to_attach += [
            f"{temp_dir}/var/log/clickhouse-server/clickhouse-server.log",
            f"{temp_dir}/var/log/clickhouse-server/clickhouse-server.err.log",
        ]
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
        run_stateless_test(
            no_parallel=no_parallel,
            no_sequiential=no_sequential,
            batch_num=batch_num,
            batch_total=total_batches,
            test=args.test,
        )
        CH.log_cluster_stop_replication()
        results.append(FTResultsProcessor(wd=temp_dir).run())
        results[-1].set_timing(stopwatch=stop_watch_)
        res = results[-1].is_ok()

    Result.create_from(
        results=results, stopwatch=stop_watch, files=logs_to_attach if not res else []
    ).complete_job()


if __name__ == "__main__":
    main()
