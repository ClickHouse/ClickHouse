import argparse
import os
import time
from pathlib import Path

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.jobs.scripts.infra_utils import BinaryFinder
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
        "--test-options",
        help="Comma separated option(s): parallel|non-parallel|BATCH_NUM/BTATCH_TOT|..",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def run_specific_tests(tests, runs=1):
    test_output_file = f"{temp_dir}/test_result.txt"
    nproc = int(Utils.cpu_count() / 2)
    command = f"./tests/clickhouse-test --testname --shard --zookeeper --check-zookeeper-session --hung-check \
        --capture-client-stacktrace --queries ./tests/queries --report-logs-stats --test-runs {runs} \
        --jobs {nproc} --order=random -- {' '.join(tests)} | ts '%Y-%m-%d %H:%M:%S' | tee -a \"{test_output_file}\""
    if Path(test_output_file).exists():
        Path(test_output_file).unlink()
    Shell.run(command, verbose=True)


def main():

    args = parse_args()
    # test_options = [o.trim() for o in args.test_options.split(",")]

    stop_watch = Utils.Stopwatch()
    stages = list(JobStages)

    build_san = "release"

    if not Path("./ci/tmp/clickhouse").exists():
        if Utils.is_arm():
            build_type = f"arm_{build_san}"
        else:
            build_type = f"amd_{build_san}"
        assert BinaryFinder.find_first_existing_artifact(build_type, download=True)

    Shell.check("chmod 755 ./ci/tmp/clickhouse", strict=True, verbose=True)

    ch_path = temp_dir
    config_installs_args = ""
    commands = [
        f"rm -rf {temp_dir}/etc",
        f"rm -rf {temp_dir}/var/log && mkdir -p {temp_dir}/var/log/clickhouse-server",
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
        f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g;' $(readlink -f $file); done",
        f"for file in {temp_dir}/etc/clickhouse-server/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|>/var/log|>{temp_dir}/var/log|g; s|>/etc/|>{temp_dir}/etc/|g;' $(readlink -f $file); done",
        f"for file in {temp_dir}/etc/clickhouse-server/config.d/*.xml; do [ -f $file ] && echo Change config $file && sed -i 's|<path>local_disk|<path>{temp_dir}/local_disk|g' $(readlink -f $file); done",
        f"clickhouse-server --version",
    ]

    res = True
    results = []
    CH = ClickHouseProc()

    # thread fuzzing
    os.environ["IS_FLAKY_CHECK"] = "1"
    os.environ["THREAD_FUZZER_CPU_TIME_PERIOD_US"] = "1000"
    os.environ["THREAD_FUZZER_SLEEP_PROBABILITY"] = "0.1"
    os.environ["THREAD_FUZZER_SLEEP_TIME_US_MAX"] = "100000"
    Utils.add_to_PATH(f"./tests")
    Utils.add_to_PATH(f"./ci/tmp")

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        results.append(
            Result.from_commands_run(
                name=JobStages.INSTALL_CLICKHOUSE, command=commands
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.START in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        minio_log = f"{temp_dir}/minio.log"

        # subprocess.Popen("sudo chroot ./ci/tmp /clickhouse server -- --logger.stderr /var/log/clickhouse-server/stderr1.log --logger.log /var/log/clickhouse-server/clickhouse-server1.log --logger.errorlog /var/log/clickhouse-server/clickhouse-server1.err.log", stderr=subprocess.STDOUT, shell=True)
        res = res and CH.start()
        res = res and CH.start_minio(test_type="stateless", log_file_path=minio_log)
        time.sleep(10)
        Shell.check("ps -ef | grep minio", verbose=True)
        res = res and Shell.check(
            "aws s3 ls s3://test --endpoint-url http://localhost:11111/", verbose=True
        )
        res = res and CH.wait_ready()
        if res:
            print("ch started")

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
        # assert Shell.check(
        #     "clickhouse-client -q \"insert into system.zookeeper (name, path, value) values ('auxiliary_zookeeper2', '/test/chroot/', '')\"",
        #     verbose=True,
        # )
        run_specific_tests(
            tests=["03036_dynamic_read_shared_subcolumns_memory.s"],
            runs=50,
        )
        results.append(FTResultsProcessor(wd=temp_dir).run())
        results[-1].set_timing(stopwatch=stop_watch_)
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch, files=[]).complete_job()


if __name__ == "__main__":
    main()
