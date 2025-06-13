import argparse
import csv
import os
import re
import subprocess
import time
import traceback
from datetime import datetime
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"
perf_wd = f"{temp_dir}/perf_wd"
db_path = f"{perf_wd}/db0"
perf_right = f"{perf_wd}/right"
perf_left = f"{perf_wd}/left"
perf_right_config = f"{perf_right}/config"
perf_left_config = f"{perf_left}/config"

GET_HISTORICAL_TRESHOLDS_QUERY = """\
select test, query_index,
    quantileExact(0.99)(abs(diff)) * 1.5 AS max_diff,
    quantileExactIf(0.99)(stat_threshold, abs(diff) < stat_threshold) * 1.5 AS max_stat_threshold,
    query_display_name
from query_metrics_v2
-- We use results at least one week in the past, so that the current
-- changes do not immediately influence the statistics, and we have
-- some time to notice that something is wrong.
-- TODO: switch 3 month to 1 month once we have data in the table
where event_date between now() - interval 3 month - interval 1 week
    and now() - interval 1 week
    and metric = 'client_time'
    and pr_number = 0
group by test, query_index, query_display_name
having count(*) > 100"""

INSERT_HISTORICAL_DATA = """\
INSERT INTO query_metrics_v2
SELECT
    '{EVENT_DATE}' AS event_date,
    '{EVENT_DATE_TIME}' AS event_time,
    {PR_NUMBER} AS pr_number,
    '{REF_SHA}' AS old_sha,
    '{CUR_SHA}' AS new_sha,
    test,
    query_index,
    query_display_name,
    metric_name AS metric,
    old_value,
    new_value,
    diff,
    stat_threshold
FROM input(
    'metric_name String,
     old_value Float64,
     new_value Float64,
     diff Float64,
     ratio_display_text String,
     stat_threshold Float64,
     test String,
     query_index Int32,
     query_display_name String'
) FORMAT TSV"""

# Precision is going to be 1.5 times worse for PRs, because we run the queries
# less times. How do I know it? I ran this:
# SELECT quantilesExact(0., 0.1, 0.5, 0.75, 0.95, 1.)(p / m)
# FROM
# (
#     SELECT
#         quantileIf(0.95)(stat_threshold, pr_number = 0) AS m,
#         quantileIf(0.95)(stat_threshold, (pr_number != 0) AND (abs(diff) < stat_threshold)) AS p
#     FROM query_metrics_v2
#     WHERE (event_date > (today() - toIntervalMonth(1))) AND (metric = 'client_time')
#     GROUP BY
#         test,
#         query_index,
#         query_display_name
#     HAVING count(*) > 100
# )
#
# The file can be empty if the server is inaccessible, so we can't use
# TSVWithNamesAndTypes.


class JobStages(metaclass=MetaClasses.WithIter):
    INSTALL_CLICKHOUSE = "install"
    INSTALL_CLICKHOUSE_REFERENCE = "install_reference"
    DOWNLOAD_DATASETS = "download"
    CONFIGURE = "configure"
    RESTART = "restart"
    TEST = "queries"
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
            serever_path = f"{temp_dir}/perf_wd/left"
            log_file = f"{serever_path}/server.log"
        else:
            server_port = self.RIGHT_SERVER_PORT
            keeper_port = self.RIGHT_SERVER_KEEPER_PORT
            raft_port = self.RIGHT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.RIGHT_SERVER_INTERSERVER_PORT
            serever_path = f"{temp_dir}/perf_wd/right"
            log_file = f"{serever_path}/server.log"

        self.preconfig_start_cmd = f"{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml -- --path {db_path} --user_files_path {db_path}/user_files --top_level_domains_path {perf_wd}/top_level_domains --keeper_server.storage_path {temp_dir}/coordination0 --tcp_port {server_port}"
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

        Shell.check(
            f"clickhouse-client --port {self.port} --query 'create database IF NOT EXISTS test' && clickhouse-client --port {self.port} --query 'rename table datasets.hits_v1 to test.hits'",
            verbose=True,
        )
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
        cls, test_file, runs=7, max_queries=0, results_path=f"{temp_dir}/perf_wd/"
    ):
        test_name = test_file.split("/")[-1].removesuffix(".xml")
        sw = Utils.Stopwatch()
        res, out, err = Shell.get_res_stdout_stderr(
            f"./tests/performance/scripts/perf.py --host localhost localhost \
                --port {cls.LEFT_SERVER_PORT} {cls.RIGHT_SERVER_PORT} \
                --runs {runs} --max-queries {max_queries} \
                --profile-seconds 10 \
                {test_file}",
            verbose=True,
            strip=False,
        )
        duration = sw.duration
        if res != 0:
            with open(f"{results_path}/{test_name}-err.log", "w") as f:
                f.write(err)
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
    parser.add_argument("--ch-path", help="Path to clickhouse binary", default=temp_dir)
    parser.add_argument(
        "--test-options",
        help="Comma separated option(s) BATCH_NUM/BTATCH_TOT|?",
        default="",
    )
    parser.add_argument("--param", help="Optional job start stage", default=None)
    parser.add_argument("--test", help="Optional test name pattern", default="")
    return parser.parse_args()


def find_prev_build(info, build_type):
    commits = info.get_custom_data("previous_commits_sha") or []

    for sha in commits:
        link = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/{build_type}/clickhouse"
        if Shell.check(f"curl -sfI {link} > /dev/null"):
            return link

    return None


def main():

    args = parse_args()
    test_options = args.test_options.split(",")
    batch_num, total_batches = 1, 1
    compare_against_master = False
    compare_against_release = False
    for test_option in test_options:
        if "/" in test_option:
            batch_num, total_batches = map(int, test_option.split("/"))
        if test_option == "master_head":
            compare_against_master = True
        elif test_option == "prev_release":
            compare_against_release = True

    batch_num -= 1
    assert 0 <= batch_num < total_batches and total_batches >= 1

    assert (
        compare_against_master or compare_against_release
    ), "test option: head_master or prev_release must be selected"

    # release_version = CHVersion.get_release_version_as_dict()
    info = Info()

    if Utils.is_arm():
        if compare_against_master:
            if info.git_branch == "master":
                link_for_ref_ch = find_prev_build(info, "build_arm_release")
                assert link_for_ref_ch, "previous clickhouse build has not been found"
            else:
                link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        elif compare_against_release:
            # TODO:
            # link_for_ref_ch = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/{release_version['major']}.{release_version['minor']-1}/{release_version['githash']}/build_arm_release/clickhouse"
            assert False
        else:
            assert False
    elif Utils.is_amd():
        if compare_against_master:
            if info.git_branch == "master":
                link_for_ref_ch = find_prev_build(info, "build_amd_release")
                assert link_for_ref_ch, "previous clickhouse build has not been found"
            else:
                link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        elif compare_against_release:
            # TODO:
            # link_for_ref_ch = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/{release_version['major']}.{release_version['minor']-1}/{release_version['githash']}/build_amd_release/clickhouse"
            assert False
        else:
            assert False
    else:
        Utils.raise_with_error(f"Unknown processor architecture")

    test_keyword = args.test

    ch_path = args.ch_path
    assert (
        Path(ch_path + "/clickhouse").is_file()
        or Path(ch_path + "/clickhouse").is_symlink()
    ), f"clickhouse binary not found in [{ch_path}]"

    stop_watch = Utils.Stopwatch()
    stages = list(JobStages)

    logs_to_attach = []
    report_files = [
        f"{temp_dir}/perf_wd/report.html",
        f"{temp_dir}/perf_wd/all-queries.html",
    ]

    stage = args.param or JobStages.INSTALL_CLICKHOUSE
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    results = []

    # add right CH location to PATH
    Utils.add_to_PATH(perf_right)
    # TODO:
    # Set python output encoding so that we can print queries with non-ASCII letters.
    # export PYTHONIOENCODING=utf-8

    if res and JobStages.INSTALL_CLICKHOUSE in stages:
        print("Install ClickHouse")
        commands = [
            f"mkdir -p {perf_right_config}",
            f"cp ./programs/server/config.xml {perf_right_config}",
            f"cp ./programs/server/users.xml {perf_right_config}",
            f"cp -r --dereference ./programs/server/config.d {perf_right_config}",
            f"cp ./tests/performance/scripts/config/config.d/*xml {perf_right_config}/config.d/",
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
            Result.from_commands_run(name="Install ClickHouse", command=commands)
        )
        res = results[-1].is_ok()

    reference_sha = ""
    if res and JobStages.INSTALL_CLICKHOUSE_REFERENCE in stages:
        print("Install Reference")
        if not Path(f"{perf_left}/.done").is_file():
            commands = [
                f"mkdir -p {perf_left_config}",
                f"wget -nv -P {perf_left}/ {link_for_ref_ch}",
                f"chmod +x {perf_left}/clickhouse",
                f"cp -r ./tests/performance {perf_left}/",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-local",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-client",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-server",
                f"ln -sf {perf_left}/clickhouse {perf_left}/clickhouse-keeper",
            ]
            results.append(
                Result.from_commands_run(
                    name="Install Reference ClickHouse", command=commands
                )
            )
            reference_sha = Shell.get_output(
                f"{perf_left}/clickhouse -q \"SELECT value FROM system.build_options WHERE name='GIT_HASH'\""
            )
            res = results[-1].is_ok()
            Shell.check(f"touch {perf_left}/.done")

    if res and not info.is_local_run:

        def prepare_historical_data():
            cidb = CIDBCluster()
            assert cidb.is_ready()
            result = cidb.do_select_query(
                query=GET_HISTORICAL_TRESHOLDS_QUERY, timeout=10, retries=3
            )
            with open(
                f"{perf_wd}/historical-thresholds.tsv", "w", encoding="utf-8"
            ) as f:
                f.write(result)

        results.append(
            Result.from_commands_run(
                name="Select historical data", command=prepare_historical_data
            )
        )
        res = results[-1].is_ok()
    elif info.is_local_run:
        print(
            "Skip historical data check for local runs to avoid dependencies on CIDB and secrets"
        )
        Shell.check(f"touch {perf_wd}/historical-thresholds.tsv", verbose=True)

    if res and JobStages.DOWNLOAD_DATASETS in stages:
        print("Download datasets")
        if not Path(f"{db_path}/.done").is_file():
            Shell.check(f"mkdir -p {db_path}", verbose=True)
            dataset_paths = {
                "hits10": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_10m_single.tar",
                "hits100": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_100m_single.tar",
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
            time.sleep(5)
            Shell.check("ps -ef | grep clickhouse", verbose=True)
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
            f"rm -rf {perf_left}/db {perf_right}/db",
            f"rm -rf {db_path}/preprocessed_configs {db_path}/data/system {db_path}/metadata/system {db_path}/status",
            f"cp -al {db_path} {perf_left}/db ||:",
            f"cp -al {db_path} {perf_right}/db ||:",
            f"cp -R {temp_dir}/coordination0 {perf_left}/coordination",
            f"cp -R {temp_dir}/coordination0 {perf_right}/coordination",
        ]
        results.append(Result.from_commands_run(name="Configure", command=commands))
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
        print("Tests")
        test_files = [
            file for file in os.listdir("./tests/performance/") if file.endswith(".xml")
        ]
        # TODO: in PRs filter test files against changed files list if only tests has been changed
        # changed_files = info.get_custom_data("changed_files")
        if test_keyword:
            test_files = [file for file in test_files if test_keyword in file]
        else:
            test_files = test_files[batch_num::total_batches]
        print(f"Job Batch: [{batch_num}/{total_batches}]")
        print(f"Test Files ({len(test_files)}): [{test_files}]")
        assert test_files

        def run_tests():
            for test in test_files:
                CHServer.run_test(
                    "./tests/performance/" + test,
                    runs=7,
                    max_queries=10,
                    results_path=perf_wd,
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

        Shell.check(f"{perf_left}/clickhouse --version  > {perf_wd}/left-commit.txt")
        Shell.check(f"git log -1 HEAD > {perf_wd}/right-commit.txt")
        os.environ["CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX"] = (
            Utils.normalize_string(info.job_name)
        )
        os.environ["CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME"] = info.job_name
        os.environ["CHPC_CHECK_START_TIMESTAMP"] = str(int(Utils.timestamp()))

        commands = [
            f"PR_TO_TEST={info.pr_number} "
            f"SHA_TO_TEST={info.sha} "
            "stage=get_profiles "
            f"{script_path}",
        ]

        results.append(
            Result.from_commands_run(
                name="Report",
                command=commands,
                workdir=perf_wd,
            )
        )

        if Path(f"{perf_wd}/ci-checks.tsv").is_file():
            # insert test cases result generated by legacy script as tsv file into praktika Result object - so that they are written into DB later
            test_results = []
            with open(f"{perf_wd}/ci-checks.tsv", "r", encoding="utf-8") as f:
                header = next(f).strip().split("\t")  # Read actual column headers
                next(f)  # Skip type line (e.g. UInt32, String...)
                reader = csv.DictReader(f, delimiter="\t", fieldnames=header)
                for row in reader:
                    if not row["test_name"]:
                        continue
                    test_results.append(
                        Result(
                            name=row["test_name"],
                            status=row["test_status"],
                            duration=float(row["test_duration_ms"]) / 1000,
                        )
                    )
            # results[-2] is a previuos subtask
            results[-2].results = test_results
        else:
            print("WARNING: compare.sh did not generate ci-checks.tsv file")

        res = results[-1].is_ok()

    if res and not info.is_local_run:

        def insert_historical_data():
            cidb = CIDBCluster()
            assert cidb.is_ready()

            now = datetime.now()
            date = now.date().isoformat()
            date_time = now.isoformat(sep=" ").split(".")[0]

            report_path = f"{perf_wd}/report/all-query-metrics.tsv"
            with open(report_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                print(lines)
                data = "".join(lines)
            print(data)

            query = INSERT_HISTORICAL_DATA.format(
                EVENT_DATE=date,
                EVENT_DATE_TIME=date_time,
                PR_NUMBER=info.pr_number,
                REF_SHA=reference_sha,
                CUR_SHA=info.sha,
            )

            print(f"Do insert historical data query: >>>\n{query}\n<<<")
            res = cidb.do_insert_query(
                query=query,
                data=data,
                timeout=10,
                retries=3,
            )
            if res:
                print(f"Inserted [{len(lines)}] lines")
            else:
                print(f"Inserted [{len(lines)}] lines - failed")
            return True

        results.append(
            Result.from_commands_run(
                name="Insert historical data",
                command=insert_historical_data,
                with_info=True,
            )
        )

    # TODO: code to fetch status was taken from old script as is - status is to be correctly set in Test stage and this stage is to be removed!
    message = ""
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
        try:
            with open(f"{perf_wd}/report.html", "r", encoding="utf-8") as report_fd:
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
    Shell.check(f"rm -f {perf_wd}/logs.tar.zst")
    Shell.check(
        f'cd {perf_wd} && find . -type f \( -name "*.log" -o -name "*.tsv" -o -name "*.txt" -o -name "*.rep" -o -name "*.svg" \) ! -path "*/db/*" !  -path "*/db0/*" -print0 | tar --null -T - -cf - | zstd -o ./logs.tar.zst',
        verbose=True,
    )
    if Path(f"{perf_wd}/logs.tar.zst").is_file():
        files_to_attach.append(f"{perf_wd}/logs.tar.zst")

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=files_to_attach + [f"{perf_wd}/report/all-query-metrics.tsv"],
        info=message,
    ).complete_job()


if __name__ == "__main__":
    main()
