import argparse
import os
import re
import subprocess
import time
import traceback
from pathlib import Path

from praktika.result import Result
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_version import CHVersion


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    INSTALL_CLICKHOUSE_REFERENCE = "install_reference"
    DOWNLOAD_DATASETS = "download"
    CONFIGURE = "configure"
    RESTART = "restart"
    TEST = "test"
    REPORT = "report"
    # TODO: stage implement code from the old script as is - refactor and remove
    CHECK_RESULTS = "check_results"


class CHServer:
    # upstream/master
    LEFT_SERVER_PORT = 9001
    LEFT_SERVER_KEEPER_PORT = 9181
    LEFT_SERVER_KEEPER_RAFT_PORT = 9234
    LEFT_SERVER_INTERSERVER_PORT = 9009
    # patched version
    RIGHT_SERVER_PORT = 19001
    RIGHT_SERVER_KEEPER_PORT = 19181
    RIGHT_SERVER_KEEPER_RAFT_PORT = 19234
    RIGHT_SERVER_INTERSERVER_PORT = 19009

    def __init__(self, is_left=False):
        if is_left:
            server_port = self.LEFT_SERVER_PORT
            keeper_port = self.LEFT_SERVER_KEEPER_PORT
            raft_port = self.LEFT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.LEFT_SERVER_INTERSERVER_PORT
            serever_path = "/tmp/praktika/perf_wd/left"
            log_file = f"{serever_path}/server.log"
        else:
            server_port = self.RIGHT_SERVER_PORT
            keeper_port = self.RIGHT_SERVER_KEEPER_PORT
            raft_port = self.RIGHT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.RIGHT_SERVER_INTERSERVER_PORT
            serever_path = "/tmp/praktika/perf_wd/right"
            log_file = f"{serever_path}/server.log"

        self.preconfig_start_cmd = f"{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml -- --path /tmp/praktika/db0 --user_files_path /tmp/praktika/db0/user_files --top_level_domains_path /tmp/praktika/perf_wd/top_level_domains --keeper_server.storage_path /tmp/praktika/coordination0 --tcp_port {server_port}"
        self.log_fd = None
        self.log_file = log_file
        self.port = server_port
        self.server_path = serever_path
        self.name = "Reference" if is_left else "Patched"

        self.start_cmd = f"{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml \
            -- --path {serever_path}/db --user_files_path {serever_path}/db/user_files \
            --top_level_domains_path {serever_path}/top_level_domains --tcp_port {server_port} \
            --keeper_server.tcp_port {keeper_port} --keeper_server.raft_configuration.server.port {raft_port} \
            --keeper_server.storage_path {serever_path}/coordination --zookeeper.node.port {keeper_port} \
            --interserver_http_port {inter_server_port}"

    def start_preconfig(self):
        print("Starting ClickHouse server")
        print("Command: ", self.preconfig_start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.preconfig_start_cmd,
            stderr=subprocess.STDOUT,
            stdout=self.log_fd,
            shell=True,
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")

        res = res and Shell.check(
            f"clickhouse-client --port {self.port} --query 'create database IF NOT EXISTS test'",
            verbose=True,
        )
        # res = res and Shell.check(f"clickhouse-client --port {self.port} --query 'rename table datasets.hits_v1 to test.hits'", verbose=True)
        return res

    def start(self):
        print(f"Starting [{self.name}] ClickHouse server")
        print("Command: ", self.start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.start_cmd, stderr=subprocess.STDOUT, stdout=self.log_fd, shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print(f"ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print(f"ClickHouse server ready")
        else:
            print(f"ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True

    def ask(self, query):
        return Shell.get_output(
            f'{self.server_path}/clickhouse-client --port {self.port} --query "{query}"'
        )

    @classmethod
    def run_test(
        cls, test_file, runs=7, max_queries=0, results_path="/tmp/praktika/perf_wd/"
    ):
        test_name = test_file.split("/")[-1].removesuffix(".xml")
        sw = Utils.Stopwatch()
        res, out, err = Shell.get_res_stdout_stderr(
            f"./tests/performance/scripts/perf.py --host localhost localhost \
                --port {cls.LEFT_SERVER_PORT} {cls.RIGHT_SERVER_PORT} \
                --runs {runs} --max-queries {max_queries} \
                --profile-seconds 0 \
                {test_file}",
            verbose=True,
        )
        duration = sw.duration
        if res != 0:
            with open(f"{results_path}/{test_name}-err.log", "w") as f:
                f.write(err)
            err = Shell.get_output(f"echo \"{err}\" | grep '{test_name}\t'")
        with open(f"{results_path}/{test_name}-raw.tsv", "w") as f:
            f.write(out)
        with open(f"{results_path}/wall-clock-times.tsv", "a") as f:
            f.write(f"{test_name}\t{duration}\n")

    def terminate(self):
        print("Terminate ClickHouse process")
        timeout = 10
        if self.proc:
            Utils.terminate_process_group(self.proc.pid)

            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
                print(f"Process {self.proc.pid} terminated gracefully.")
            except Exception:
                print(
                    f"Process {self.proc.pid} did not terminate in {timeout} seconds, killing it..."
                )
                Utils.terminate_process_group(self.proc.pid, force=True)
                self.proc.wait()  # Wait for the process to be fully killed
                print(f"Process {self.proc} was killed.")
        if self.log_fd:
            self.log_fd.close()


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Performance Tests Job")
    parser.add_argument(
        "--ch-path", help="Path to clickhouse binary", default=f"/tmp/praktika/input"
    )
    parser.add_argument(
        "--test-options",
        help="Comma separated option(s) BATCH_NUM/BTATCH_TOT|?",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def main():

    args = parse_args()
    test_options = args.test_options.split(",")
    batch_num, total_batches = 1, 1
    compare_against_master = False
    compare_against_release = False
    for test_option in test_options:
        if "/" in test_option:
            batch_num, total_batches = map(int, test_option.split("/"))
        if test_option == "head_master":
            compare_against_master = True
        elif test_option == "prev_release":
            compare_against_release = True

    batch_num -= 1
    assert 0 <= batch_num < total_batches and total_batches >= 1

    assert (
        compare_against_master or compare_against_release
    ), "test option: head_master or prev_release must be selected"

    left_major, left_minor, left_sha = CHVersion.get_latest_release_major_minor_sha()

    if Utils.is_arm():
        if compare_against_master:
            link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        elif compare_against_release:
            link_for_ref_ch = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/{left_major}.{left_minor-1}/{left_sha}/package_aarch64/clickhouse"
        else:
            assert False
    elif Utils.is_amd():
        if compare_against_master:
            link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        elif compare_against_release:
            link_for_ref_ch = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/{left_major}.{left_minor-1}/{left_sha}/package_release/clickhouse"
        else:
            assert False
    else:
        Utils.raise_with_error(f"Unknown processor architecture")

    test_keyword = args.test

    ch_path = args.ch_path
    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found under [{ch_path}]"

    stop_watch = Utils.Stopwatch()
    stages = list(JobStages)

    logs_to_attach = []
    report_files = [
        "/tmp/praktika/perf_wd/report.html",
        "/tmp/praktika/perf_wd/all-queries.html",
    ]

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    perf_wd = "/tmp/praktika/perf_wd"
    perf_right = f"{perf_wd}/right/"
    perf_left = f"{perf_wd}/left/"
    perf_right_config = f"{perf_right}/config"
    perf_left_config = f"{perf_left}/config"
    results = []

    # Shell.check(f"rm -rf {perf_wd} && mkdir -p {perf_wd}")

    # add right CH location to PATH
    Utils.add_to_PATH(perf_right)
    results_path = "/tmp/praktika/perf_wd/"
    # TODO:
    # Set python output encoding so that we can print queries with non-ASCII letters.
    # export PYTHONIOENCODING=utf-8
    # script_path="tests/performance/scripts/"
    # ulimit -c unlimited
    # cat /proc/sys/kernel/core_pattern

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        print("Install ClickHouse")
        commands = [
            f"mkdir -p {perf_right_config}",
            f"cp ./programs/server/config.xml {perf_right_config}",
            f"cp ./programs/server/users.xml {perf_right_config}",
            f"cp -r --dereference ./programs/server/config.d {perf_right_config}",
            # f"cp ./tests/performance/scripts/config/config.d/*.xml {perf_right_config}/config.d/",
            f"cp -r ./tests/performance/scripts/config/users.d {perf_right_config}/users.d",
            f"cp -r ./tests/config/top_level_domains {perf_wd}",
            # f"cp -r ./tests/performance {perf_right}",
            f"chmod +x {ch_path}/clickhouse",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-server",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-local",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-client",
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse-keeper",
            "clickhouse-local --version",
        ]
        results.append(
            Result.from_commands_run(
                name="Install ClickHouse", command=commands, with_log=True
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.INSTALL_CLICKHOUSE_REFERENCE in stages:
        print("Install Reference")
        if not Path(f"{perf_left}/.done").is_file():
            # TODO: use config from the same sha as reference CH binary
            # git checkout left_sha
            # rm -rf /tmp/praktika/left && mkdir -p /tmp/praktika/left
            # cp -r ./tests/config /tmp/praktika/left/config
            # git checkout -
            commands = [
                f"mkdir -p {perf_left_config}",
                f"wget -nv -P {perf_left}/ {link_for_ref_ch}",
                f"chmod +x {perf_left}/clickhouse",
                f"cp -r ./tests/performance /tmp/praktika/left/",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-local",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-client",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-server",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-keeper",
            ]
            results.append(
                Result.from_commands_run(
                    name="Install Reference ClickHouse", command=commands, with_log=True
                )
            )
            res = results[-1].is_ok()
            Shell.check(f"touch {perf_left}/.done")

    if res and JobStages.DOWNLOAD_DATASETS in stages:
        print("Download datasets")
        db_path = "/tmp/praktika/db0/"

        if not Path(f"{db_path}/.done").is_file():
            Shell.check(f"mkdir -p {db_path}", verbose=True)
            datasets = ["hits1", "hits10", "hits100", "values"]
            dataset_paths = {
                "hits10": "https://clickhouse-private-datasets.s3.amazonaws.com/hits_10m_single/partitions/hits_10m_single.tar",
                "hits100": "https://clickhouse-private-datasets.s3.amazonaws.com/hits_100m_single/partitions/hits_100m_single.tar",
                "hits1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
                "values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
            }
            cmds = []
            for dataset_path in dataset_paths.values():
                cmds.append(
                    f'wget -nv -nd -c "{dataset_path}" -O- | tar --extract --verbose -C {db_path}'
                )
            res = Shell.check_parallel(cmds, verbose=True)
            results.append(
                Result(
                    name="Download datasets",
                    status=Result.Status.SUCCESS if res else Result.Status.ERROR,
                )
            )
            if res:
                Shell.check(f"touch {db_path}/.done")

    if res and JobStages.CONFIGURE in stages:
        print("Configure")

        leftCH = CHServer(is_left=True)

        def restart_ch():
            res_ = leftCH.start_preconfig()
            leftCH.terminate()
            # wait for termination
            time.sleep(2)
            Shell.check("ps -ef | grep clickhouse")
            return res_

        commands = [
            f'echo "ATTACH DATABASE default ENGINE=Ordinary" > {db_path}/metadata/default.sql',
            f'echo "ATTACH DATABASE datasets ENGINE=Ordinary" > {db_path}/metadata/datasets.sql',
            f"ls {db_path}/metadata",
            f"rm {perf_right_config}/config.d/text_log.xml ||:",
            # backups disk uses absolute path, and this overlaps between servers, that could lead to errors
            f"rm {perf_right_config}/config.d/backups.xml ||:",
            f"cp -rv {perf_right_config} {perf_left}/",
            restart_ch,
            # Make copies of the original db for both servers. Use hardlinks instead
            # of copying to save space. Before that, remove preprocessed configs and
            # system tables, because sharing them between servers with hardlinks may
            # lead to weird effects
            f"rm -rf {perf_left}/db",
            f"rm -rf {perf_right}/db",
            f"rm -r {db_path}/preprocessed_configs",
            f"rm -r {db_path}/data/system",
            f"rm -r {db_path}/metadata/system",
            f"rm -rf {db_path}/status",
            f"cp -al {db_path} {perf_left}/db",
            f"cp -al {db_path} {perf_right}/db",
            f"cp -R /tmp/praktika/coordination0 {perf_left}/coordination",
            f"cp -R /tmp/praktika/coordination0 {perf_right}/coordination",
        ]
        results.append(
            Result.from_commands_run(name="Configure", command=commands, with_log=True)
        )
        res = results[-1].is_ok()

    leftCH = CHServer(is_left=True)
    rightCH = CHServer(is_left=False)

    if res and JobStages.RESTART in stages:
        print("Start Servers")

        def restart_ch1():
            res_ = leftCH.start()
            return res_

        def restart_ch2():
            res_ = rightCH.start()
            return res_

        commands = [
            restart_ch1,
            restart_ch2,
        ]
        results.append(Result.from_commands_run(name="Start", command=commands))
        # TODO : check datasets are loaded:
        print(
            leftCH.ask(
                "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
            )
        )
        print(leftCH.ask("select * from system.build_options"))
        print(
            rightCH.ask(
                "select * from system.tables where database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
            )
        )
        print(rightCH.ask("select * from system.build_options"))
        res = results[-1].is_ok()
        if not res:
            logs = []
            if Path(rightCH.log_file).is_file():
                logs.append(rightCH.log_file)
            if Path(leftCH.log_file).is_file():
                logs.append(leftCH.log_file)
            results[-1].set_files(logs)

    if res and JobStages.TEST in stages:
        print("Run Tests")
        test_files = [
            file for file in os.listdir("./tests/performance/") if file.endswith(".xml")
        ]
        if test_keyword:
            test_files = [file for file in test_files if test_keyword in file]
        else:
            test_files = test_files[batch_num::total_batches]
        print(f"Job Batch: [{batch_num}/{total_batches}]")
        print(f"Test Files ({len(test_files)}): [{test_files}]")
        assert test_files

        def run_tests():
            for test in test_files[batch_num::total_batches]:
                CHServer.run_test(
                    "./tests/performance/" + test,
                    runs=7,
                    max_queries=10,
                    results_path=results_path,
                )
            return True

        commands = [
            run_tests,
        ]
        results.append(Result.from_commands_run(name="Tests", command=commands))
        res = results[-1].is_ok()

    # TODO: refactor to use native Praktika report from Result and remove
    if res and JobStages.REPORT in stages:
        print("Build Report")
        script_path = Shell.get_output(
            "readlink -f ./ci/jobs/scripts/perf/compare.sh", strict=True
        )

        left_major, left_minor, left_sha = (
            CHVersion.get_latest_release_major_minor_sha()
        )
        Shell.check(
            f"/tmp/praktika/perf_wd/left/clickhouse --version  > {results_path}/left-commit.txt"
        )
        Shell.check(f"git log -1 HEAD > {results_path}/right-commit.txt")

        commands = [
            f"stage=get_profiles {script_path}",
        ]
        results.append(
            Result.from_commands_run(
                name="Report",
                command=commands,
                with_log=True,
                workdir=results_path,
            )
        )
        res = results[-1].is_ok()

    # TODO: code to fetch status was taken from old script as is - status is to be correctly set in Test stage and this stage is to be removed!
    if res and JobStages.CHECK_RESULTS in stages:

        def too_many_slow(msg):
            match = re.search(r"(|.* )(\d+) slower.*", msg)
            # This threshold should be synchronized with the value in
            # https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/performance-comparison/report.py#L629
            threshold = 5
            return int(match.group(2).strip()) > threshold if match else False

        # Try to fetch status from the report.
        sw = Utils.Stopwatch()
        status = ""
        message = ""
        try:
            with open(
                "/tmp/praktika/perf_wd/report.html", "r", encoding="utf-8"
            ) as report_fd:
                report_text = report_fd.read()
                status_match = re.search("<!--[ ]*status:(.*)-->", report_text)
                message_match = re.search("<!--[ ]*message:(.*)-->", report_text)
            if status_match:
                status = status_match.group(1).strip()
            if message_match:
                message = message_match.group(1).strip()
            # TODO: Remove me, always green mode for the first time, unless errors
            status = Result.Status.SUCCESS
            if "errors" in message.lower() or too_many_slow(message.lower()):
                status = Result.Status.FAILED
            # TODO: Remove until here
        except Exception:
            traceback.print_exc()
            status = Result.Status.FAILED
            message = "Failed to parse the report."

        if not status:
            status = Result.Status.FAILED
            message = "No status in report."
        elif not message:
            status = Result.Status.FAILED
            message = "No message in report."
        results.append(
            Result(
                name="Check Results", status=status, info=message, duration=sw.duration
            )
        )

    # Shell.check("find /tmp/praktika -type f")

    # Stop the servers to free memory. Normally they are restarted before getting
    # the profile info, so they shouldn't use much, but if the comparison script
    # fails in the middle, this might not be the case.
    # for _ in {1..30}
    # do
    # pkill clickhouse || break
    # sleep 1
    # done

    # dmesg -T > dmesg.log
    #
    # ls -lath
    #
    # 7z a '-x!*/tmp' /output/output.7z ./*.{log,tsv,html,txt,rep,svg,columns} \
    #    {right,left}/{performance,scripts} {{right,left}/db,db0}/preprocessed_configs \
    #    report analyze benchmark metrics \
    #    ./*.core.dmp ./*.core

    ## If the files aren't same, copy it
    # cmp --silent compare.log /tmp/praktika/compare.log || \
    #  cp compare.log /output

    files_to_attach = []
    if res:
        files_to_attach += logs_to_attach
    for report in report_files:
        if Path(report).exists():
            files_to_attach.append(report)

    # attach all logs with errors
    Shell.check("rm -f /tmp/praktika/perf_wd/logs.zip")
    Shell.check(
        'find /tmp/praktika/perf_wd -type f \( -name "*err*.log" -o -name "*err*.tsv" \) -exec zip /tmp/praktika/perf_wd/logs.zip {} +',
        verbose=True,
    )
    if Path("/tmp/praktika/perf_wd/logs.zip").is_file():
        files_to_attach.append("/tmp/praktika/perf_wd/logs.zip")

    Result.create_from(
        results=results, stopwatch=stop_watch, files=files_to_attach
    ).complete_job()


if __name__ == "__main__":
    main()
