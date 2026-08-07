import argparse
import csv
import os
import re
import subprocess
import time
import traceback
import urllib.parse
from datetime import datetime
from pathlib import Path

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import MetaClasses, Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"
perf_wd = f"{temp_dir}/perf_wd"
db_path = f"{perf_wd}/db0"
perf_right = f"{perf_wd}/right"
perf_left = f"{perf_wd}/left"
perf_right_config = f"{perf_right}/config"
perf_left_config = f"{perf_left}/config"
raw_query_metrics_path = f"{perf_wd}/analyze/raw-query-metrics-upload.tsv"

GET_HISTORICAL_TRESHOLDS_QUERY = """\
SELECT test, query_index,
    quantileExact(0.99)(abs(diff)) * 1.5 AS max_diff,
    quantileExactIf(0.99)(stat_threshold, abs(diff) < stat_threshold) * 1.5 AS max_stat_threshold,
    any(query_display_name) AS query_display_name
FROM query_metrics_v2
-- We use results at least one week in the past, so that the current
-- changes do not immediately influence the statistics, and we have
-- some time to notice that something is wrong.
WHERE event_date BETWEEN today() - INTERVAL 1 MONTH - INTERVAL 1 WEEK AND today() - INTERVAL 1 WEEK
    AND metric = 'client_time'
    AND pr_number = 0
GROUP BY test, query_index
HAVING count() > 100"""

INSERT_HISTORICAL_DATA = """\
INSERT INTO query_metrics_v2
(
    event_date,
    event_time,
    pr_number,
    old_sha,
    new_sha,
    test,
    query_index,
    query_display_name,
    metric,
    old_value,
    new_value,
    diff,
    stat_threshold,
    arch,
    workflow_name,
    base_branch,
    report_url,
    instance_type,
    instance_id
)
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
    stat_threshold,
    '{ARCH}' AS arch,
    '{WORKFLOW_NAME}' AS workflow_name,
    '{BASE_BRANCH}' AS base_branch,
    '{REPORT_URL}' AS report_url,
    '{INSTANCE_TYPE}' AS instance_type,
    '{INSTANCE_ID}' AS instance_id
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

RAW_QUERY_METRICS_TABLE = "query_metric_runs_v1"

# --- Aggregate report tables on the play cluster --------------------------
# These capture everything that used to only live in the static HTML report
# (Test Times, Test Performance Changes, Backward-incompatible queries,
# Skipped tests, Run errors, async Metric Changes, Tested commits) plus the
# collapsed flamegraph stacks. See ci/jobs/scripts/perf/README-db.md for DDL.

TEST_TIMES_TABLE = "perf_test_times_v1"
TEST_PERF_CHANGES_TABLE = "perf_test_perf_changes_v1"
PARTIAL_QUERIES_TABLE = "perf_partial_queries_v1"
SKIPPED_TESTS_TABLE = "perf_skipped_tests_v1"
RUN_ERRORS_TABLE = "perf_run_errors_v1"
METRIC_CHANGES_TABLE = "perf_metric_changes_v1"
FLAMEGRAPH_STACKS_TABLE = "perf_flamegraph_stacks_v1"

ch_uploads_dir = f"{perf_wd}/analyze/ch-uploads"
flamegraph_upload_path = f"{ch_uploads_dir}/flamegraph-stacks.tsv"

# Common per-row metadata columns and their SELECT expressions. Placeholders
# are filled by get_insert_metadata() + job/PR info via .format() below.
COMMON_META_COLUMNS = [
    "event_date",
    "check_start_time",
    "pr_number",
    "old_sha",
    "new_sha",
    "arch",
    "baseline_kind",
    "workflow_name",
    "base_branch",
    "report_url",
    "instance_type",
    "instance_id",
]

COMMON_META_SELECT = """\
    '{EVENT_DATE}' AS event_date,
    '{CHECK_START_TIME}' AS check_start_time,
    {PR_NUMBER} AS pr_number,
    '{REF_SHA}' AS old_sha,
    '{CUR_SHA}' AS new_sha,
    '{ARCH}' AS arch,
    '{BASELINE_KIND}' AS baseline_kind,
    '{WORKFLOW_NAME}' AS workflow_name,
    '{BASE_BRANCH}' AS base_branch,
    '{REPORT_URL}' AS report_url,
    '{INSTANCE_TYPE}' AS instance_type,
    '{INSTANCE_ID}' AS instance_id"""


def _make_insert_query(table, table_columns, input_schema, select_exprs, where=None):
    """Build INSERT INTO {table} ... SELECT ... FROM input('...') [WHERE ...] FORMAT TSV.

    `table_columns` is the list of non-metadata column names written to in the
    target table; `select_exprs` is the parallel list of SELECT expressions
    (can be bare column names or arbitrary expressions referring to columns
    produced by input()).
    """
    all_cols = COMMON_META_COLUMNS + list(table_columns)
    select_all = COMMON_META_SELECT + ",\n    " + ",\n    ".join(select_exprs)
    where_clause = f"WHERE {where}" if where else ""
    return (
        f"INSERT INTO {table}\n"
        f"(\n    " + ",\n    ".join(all_cols) + "\n)\n"
        f"SELECT\n" + select_all + "\n"
        f"FROM input('" + input_schema + "')\n"
        + where_clause + "\n"
        "FORMAT TSV"
    )


# --- Per-table configs for aggregate report uploads -----------------------
# Each entry describes how to ingest one TSV produced by compare.sh::report()
# into one table on the play cluster.

REPORT_UPLOADS = [
    {
        "table": TEST_TIMES_TABLE,
        "source": f"{perf_wd}/report/test-times.tsv",
        "table_columns": [
            "test",
            "wall_clock_sec",
            "total_client_sec",
            "queries",
            "longest_query_sec",
            "avg_query_sec",
            "shortest_query_sec",
            "runs",
        ],
        "input_schema": (
            "test String, wall_clock_sec Float64, total_client_sec Float64, "
            "queries UInt32, longest_query_sec Float64, avg_query_sec Float64, "
            "shortest_query_sec Float64, runs UInt32"
        ),
        "select_exprs": [
            "test",
            "wall_clock_sec",
            "total_client_sec",
            "queries",
            "longest_query_sec",
            "avg_query_sec",
            "shortest_query_sec",
            "runs",
        ],
        # Skip the aggregate 'Total' row that compare.sh appends - UI can sum.
        "where": "test != 'Total'",
    },
    {
        "table": TEST_PERF_CHANGES_TABLE,
        "source": f"{perf_wd}/report/test-perf-changes.tsv",
        "table_columns": [
            "test",
            "times_speedup",
            "queries",
            "bad",
            "changed",
            "unstable",
        ],
        "input_schema": (
            "test String, times_speedup_str String, queries UInt32, "
            "bad UInt32, changed UInt32, unstable UInt32"
        ),
        # compare.sh emits times_speedup as a display string:
        #   "-N.NNNx" => speedup, the magnitude is the times_speedup factor
        #   "+N.NNNx" => slowdown, the magnitude is 1 / times_speedup
        # Recover a signed Float64 so the UI can sort numerically.
        "select_exprs": [
            "test",
            (
                "multiIf("
                "startsWith(times_speedup_str, '-'), "
                "toFloat64OrZero(substring(times_speedup_str, 2, length(times_speedup_str) - 2)), "
                "startsWith(times_speedup_str, '+'), "
                "1.0 / nullIf(toFloat64OrZero(substring(times_speedup_str, 2, length(times_speedup_str) - 2)), 0), "
                "1.0) AS times_speedup"
            ),
            "queries",
            "bad",
            "changed",
            "unstable",
        ],
        "where": "test != 'Total'",
    },
    {
        "table": PARTIAL_QUERIES_TABLE,
        "source": f"{perf_wd}/report/partial-queries-report.tsv",
        "table_columns": [
            "test",
            "query_index",
            "query_display_name",
            "median_sec",
            "relative_time_stddev",
        ],
        # compare.sh column order is: time (median), rel_stddev, test, query_index, display
        "input_schema": (
            "median_sec Float64, relative_time_stddev Float64, "
            "test String, query_index Int32, query_display_name String"
        ),
        "select_exprs": [
            "test",
            "query_index",
            "query_display_name",
            "median_sec",
            "relative_time_stddev",
        ],
    },
    {
        "table": SKIPPED_TESTS_TABLE,
        "source": f"{perf_wd}/analyze/skipped-tests.tsv",
        "table_columns": ["test", "reason"],
        "input_schema": "test String, reason String",
        "select_exprs": ["test", "reason"],
    },
    {
        "table": RUN_ERRORS_TABLE,
        "source": f"{perf_wd}/run-errors.tsv",
        "table_columns": ["test", "error"],
        "input_schema": "test String, error String",
        "select_exprs": ["test", "error"],
    },
    {
        "table": METRIC_CHANGES_TABLE,
        "source": f"{perf_wd}/metrics/changes.tsv",
        "table_columns": [
            "metric",
            "old_median",
            "new_median",
            "diff",
            "times_diff",
        ],
        "input_schema": (
            "metric String, old_median Float64, new_median Float64, "
            "diff Float64, times_diff Float64"
        ),
        "select_exprs": [
            "metric",
            "old_median",
            "new_median",
            "diff",
            "times_diff",
        ],
    },
]

INSERT_FLAMEGRAPH_STACKS = """\
INSERT INTO {FLAMEGRAPH_STACKS_TABLE}
(
""" + ",\n".join("    " + c for c in COMMON_META_COLUMNS) + """,
    test,
    query_index,
    query_display_name,
    side,
    trace_type,
    stack,
    samples
)
SELECT
""" + COMMON_META_SELECT + """,
    test,
    query_index,
    query_display_name,
    side,
    trace_type,
    stack,
    samples
FROM input(
    'test String, query_index Int32, query_display_name String,
     side String, trace_type String, stack String, samples UInt64'
) FORMAT TSV"""

# clickhouse-local query that merges report/stacks.left.tsv and
# report/stacks.right.tsv into a single upload-ready TSV with an explicit
# `side` column.
BUILD_FLAMEGRAPH_UPLOAD_QUERY = """
create table flamegraph_stacks_upload engine File(TSV, 'analyze/ch-uploads/flamegraph-stacks.tsv') as
select test, query_index, query_display_name, side, trace_type, stack, samples
from (
    select test, query_index, query_display_name,
        'baseline' as side, trace_type,
        readable_trace as stack, c as samples
    from file('report/stacks.left.tsv', TSV,
        'test String, query_index Int32, trace_type String, query_display_name String, readable_trace String, c UInt64')
    union all
    select test, query_index, query_display_name,
        'candidate' as side, trace_type,
        readable_trace as stack, c as samples
    from file('report/stacks.right.tsv', TSV,
        'test String, query_index Int32, trace_type String, query_display_name String, readable_trace String, c UInt64')
)
"""

INSERT_RAW_QUERY_METRICS_DATA = """\
INSERT INTO {RAW_QUERY_METRICS_TABLE}
(
    event_date,
    check_start_time,
    pr_number,
    old_sha,
    new_sha,
    arch,
    baseline_kind,
    workflow_name,
    base_branch,
    report_url,
    instance_type,
    instance_id,
    test,
    query_index,
    query_display_name,
    side,
    query_id,
    metric_name,
    metric_value
)
SELECT
    '{EVENT_DATE}' AS event_date,
    '{CHECK_START_TIME}' AS check_start_time,
    {PR_NUMBER} AS pr_number,
    '{REF_SHA}' AS old_sha,
    '{CUR_SHA}' AS new_sha,
    '{ARCH}' AS arch,
    '{BASELINE_KIND}' AS baseline_kind,
    '{WORKFLOW_NAME}' AS workflow_name,
    '{BASE_BRANCH}' AS base_branch,
    '{REPORT_URL}' AS report_url,
    '{INSTANCE_TYPE}' AS instance_type,
    '{INSTANCE_ID}' AS instance_id,
    test,
    query_index,
    query_display_name,
    if(version = 0, 'baseline', 'candidate') AS side,
    query_id,
    metric_name,
    metric_value
FROM input(
    'test String,
     query_index Int32,
     query_display_name String,
     metric_name String,
     version UInt8,
     query_id String,
     metric_value Float64'
) FORMAT TSV"""

BUILD_RAW_QUERY_METRICS_QUERY = """\
create view query_display_names as
    select *
    from file('analyze/query-display-names.tsv', TSV,
        'test text, query_index int, query_display_name text');

create table raw_query_metrics_tsv engine File(TSV, 'analyze/raw-query-metrics-upload.tsv')
as select
    denorm.test,
    denorm.query_index,
    ifNull(query_display_names.query_display_name, '') as query_display_name,
    denorm.metric_name,
    denorm.version,
    denorm.query_id,
    denorm.metric_value
from file('analyze/query-run-metrics-denorm.tsv', TSV,
    'test text, query_index int, metric_name text, version UInt8, query_id text, metric_value float') denorm
left join query_display_names using (test, query_index)
order by denorm.test, denorm.query_index, denorm.metric_name, denorm.version, denorm.query_id
"""

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


def escape_sql_string(value):
    if value is None:
        return ""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
    )


def get_perf_arch():
    if Utils.is_arm():
        return "arm"
    if Utils.is_amd():
        return "amd"
    Utils.raise_with_error("Unknown processor architecture")


def build_perf_query_history_link(test_name, check_name):
    """Build a ClickHouse Play link showing performance history for a query on master."""
    table = Settings.CI_DB_TABLE_NAME or "checks"
    tn = (test_name or "").replace("'", "''")
    cn = (check_name or "").replace("'", "''")
    query = f"""\
SELECT
    check_start_time,
    commit_sha AS commit,
    test_name AS test,
    test_duration_ms AS ms,
    report_url
FROM {table}
WHERE pull_request_number = 0
    AND check_name LIKE '{cn}'
    AND check_start_time >= now() - INTERVAL 14 DAY
    AND test_name = '{tn}'
ORDER BY test, check_start_time
"""
    base = Settings.CI_DB_READ_URL or ""
    user = Settings.CI_DB_READ_USER or ""
    if user:
        sep = "&" if "?" in base else "?"
        base = f"{base}/play{sep}user={urllib.parse.quote(user, safe='')}&run=1"
    return f"{base}#{Utils.to_base64(query)}"


def get_insert_metadata(info, compare_against_release):
    return {
        "ARCH": escape_sql_string(get_perf_arch()),
        "BASELINE_KIND": "release_base" if compare_against_release else "master_head",
        "WORKFLOW_NAME": escape_sql_string(info.workflow_name),
        "BASE_BRANCH": escape_sql_string(info.base_branch),
        "REPORT_URL": escape_sql_string(info.get_job_report_url()),
        "INSTANCE_TYPE": escape_sql_string(info.instance_type),
        "INSTANCE_ID": escape_sql_string(info.instance_id),
    }


def build_raw_query_metrics_tsv():
    Path(raw_query_metrics_path).unlink(missing_ok=True)
    result = subprocess.run(
        ["clickhouse-local", "--query", BUILD_RAW_QUERY_METRICS_QUERY],
        cwd=perf_wd,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        print(
            f"WARNING: Failed to build raw query metrics TSV with exit code [{result.returncode}]"
        )
        return False
    if not Path(raw_query_metrics_path).is_file():
        print(f"WARNING: Raw query metrics TSV [{raw_query_metrics_path}] was not created")
        return False
    return True


def build_flamegraph_upload_tsv():
    """Merge report/stacks.{left,right}.tsv into analyze/ch-uploads/flamegraph-stacks.tsv.

    Returns False (and logs a warning) if either input file is missing or
    clickhouse-local fails, so the caller can skip the upload.
    """
    left_stacks = Path(perf_wd) / "report/stacks.left.tsv"
    right_stacks = Path(perf_wd) / "report/stacks.right.tsv"
    if not left_stacks.is_file() or not right_stacks.is_file():
        print(
            f"WARNING: flamegraph stacks inputs missing "
            f"(left={left_stacks.is_file()}, right={right_stacks.is_file()}), "
            f"skipping flamegraph upload"
        )
        return False

    Path(ch_uploads_dir).mkdir(parents=True, exist_ok=True)
    Path(flamegraph_upload_path).unlink(missing_ok=True)
    result = subprocess.run(
        ["clickhouse-local", "--query", BUILD_FLAMEGRAPH_UPLOAD_QUERY],
        cwd=perf_wd,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        print(
            f"WARNING: Failed to build flamegraph stacks TSV with exit code [{result.returncode}]"
        )
        return False
    if not Path(flamegraph_upload_path).is_file():
        print(
            f"WARNING: Flamegraph stacks TSV [{flamegraph_upload_path}] was not created"
        )
        return False
    return True


def get_check_start_time():
    """Return the perf check start time (ISO, no microseconds).

    Uses the CHPC_CHECK_START_TIMESTAMP env var when available so that every
    batch of the same job lines up on the same timestamp (same "data point"
    in history charts). Falls back to now() for manual runs.
    """
    check_start_timestamp = os.environ.get("CHPC_CHECK_START_TIMESTAMP", "")
    if check_start_timestamp:
        return (
            datetime.fromtimestamp(int(check_start_timestamp))
            .isoformat(sep=" ")
            .split(".")[0]
        )
    return datetime.now().isoformat(sep=" ").split(".")[0]


def run_report_upload(cfg, cidb, info, reference_sha, compare_against_release):
    """Upload one entry from REPORT_UPLOADS to the play cluster.

    Silently skips if the source TSV is missing or empty (e.g. because the
    test stage produced no unstable queries or no skipped tests). Returns
    True on success or skip, False on upload failure.
    """
    source_path = Path(cfg["source"])
    if not source_path.is_file():
        print(f"Skipping upload to [{cfg['table']}]: [{source_path}] not found")
        return True

    with open(source_path, "r", encoding="utf-8") as f:
        data = f.read()
    if not data.strip():
        print(f"Skipping upload to [{cfg['table']}]: [{source_path}] is empty")
        return True

    query_template = _make_insert_query(
        table=cfg["table"],
        table_columns=cfg["table_columns"],
        input_schema=cfg["input_schema"],
        select_exprs=cfg["select_exprs"],
        where=cfg.get("where"),
    )
    insert_metadata = get_insert_metadata(info, compare_against_release)
    query = query_template.format(
        EVENT_DATE=datetime.now().date().isoformat(),
        CHECK_START_TIME=get_check_start_time(),
        PR_NUMBER=info.pr_number,
        REF_SHA=escape_sql_string(reference_sha),
        CUR_SHA=escape_sql_string(info.sha),
        **insert_metadata,
    )
    line_count = data.count("\n")
    print(f"Do insert into [{cfg['table']}]: >>>\n{query}\n<<<")
    insert_ok = cidb.do_insert_query(
        query=query,
        data=data,
        timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
        retries=3,
    )
    if insert_ok:
        print(f"Inserted [{line_count}] rows into [{cfg['table']}]")
    else:
        print(f"Inserted [{line_count}] rows into [{cfg['table']}] - failed")
    return insert_ok


def insert_flamegraph_stacks(cidb, info, reference_sha, compare_against_release):
    """Build and upload the merged flamegraph stacks TSV."""
    if not build_flamegraph_upload_tsv():
        return True

    with open(flamegraph_upload_path, "r", encoding="utf-8") as f:
        data = f.read()
    if not data.strip():
        print(f"Skipping flamegraph upload: [{flamegraph_upload_path}] is empty")
        return True

    insert_metadata = get_insert_metadata(info, compare_against_release)
    query = INSERT_FLAMEGRAPH_STACKS.format(
        FLAMEGRAPH_STACKS_TABLE=FLAMEGRAPH_STACKS_TABLE,
        EVENT_DATE=datetime.now().date().isoformat(),
        CHECK_START_TIME=get_check_start_time(),
        PR_NUMBER=info.pr_number,
        REF_SHA=escape_sql_string(reference_sha),
        CUR_SHA=escape_sql_string(info.sha),
        **insert_metadata,
    )
    line_count = data.count("\n")
    print(f"Do insert flamegraph stacks query: >>>\n{query}\n<<<")
    insert_ok = cidb.do_insert_query(
        query=query,
        data=data,
        timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
        retries=3,
    )
    if insert_ok:
        print(f"Inserted [{line_count}] flamegraph stack rows")
    else:
        print(f"Inserted [{line_count}] flamegraph stack rows - failed")
    return insert_ok


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
            start_new_session=True,
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
            self.start_cmd,
            stderr=subprocess.STDOUT,
            stdout=self.log_fd,
            shell=True,
            start_new_session=True,
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
    commits = info.get_kv_data("master_track_commits_sha") or []
    for sha in commits:
        link = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/{build_type}/clickhouse"
        if Shell.check(f"curl -sfI {link} > /dev/null"):
            return link
    return None


def find_base_release_build(info, build_type):
    commits = info.get_kv_data("release_branch_base_sha_with_predecessors") or []
    assert commits, "No commits found to fetch reference build"
    for sha in commits:
        link = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/{build_type}/clickhouse"
        if Shell.check(f"curl -sfI {link} > /dev/null"):
            return link
    return None


def main():

    args = parse_args()
    test_options = [to.strip() for to in args.test_options.split(",")]
    batch_num, total_batches = 1, 1
    compare_against_master = False
    compare_against_release = False
    for test_option in test_options:
        if "/" in test_option:
            batch_num, total_batches = map(int, test_option.split("/"))
        if "master_head" in test_option:
            compare_against_master = True
        elif "release_base" in test_option:
            compare_against_release = True

    batch_num -= 1
    assert 0 <= batch_num < total_batches and total_batches >= 1

    assert (
        compare_against_master or compare_against_release
    ), "test option: head_master or release_base must be selected"

    # release_version = CHVersion.get_release_version_as_dict()
    info = Info()

    if Utils.is_arm():
        if compare_against_master:
            link_for_ref_ch = find_prev_build(info, "build_arm_release")
            if not link_for_ref_ch:
                print("WARNING: No build found for master track commits, falling back to latest master build")
                link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        elif compare_against_release:
            link_for_ref_ch = find_base_release_build(info, "build_arm_release")
            assert link_for_ref_ch, "reference clickhouse build has not been found"
        else:
            assert False
    elif Utils.is_amd():
        if compare_against_master:
            link_for_ref_ch = find_prev_build(info, "build_amd_release")
            if not link_for_ref_ch:
                print("WARNING: No build found for master track commits, falling back to latest master build")
                link_for_ref_ch = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        elif compare_against_release:
            link_for_ref_ch = find_base_release_build(info, "build_amd_release")
            assert link_for_ref_ch, "reference clickhouse build has not been found"
        else:
            assert False
    else:
        Utils.raise_with_error(f"Unknown processor architecture")

    if compare_against_release:
        print("It's a comparison against latest release baseline")
        print(
            "Unshallow and Checkout on baseline sha to drop new queries that might be not supported by old version"
        )
        reference_sha = info.get_kv_data("release_branch_base_sha_with_predecessors")[0]
        Shell.check(
            f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin {info.git_branch} ||:",
            verbose=True,
        )
        Shell.check(
            f"rm -rf ./tests/performance && git checkout {reference_sha} ./tests/performance",
            verbose=True,
            strict=True,
        )

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
            f"rm {perf_right_config}/config.d/storage_conf_local.xml",  # Avoid conflicts on the filesystem cache dirs
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
            cidb = CIDBCluster(
                url="https://play.clickhouse.com?user=play", user="", pwd=""
            )
            if not cidb.is_ready():
                print(
                    "WARNING: CIDB is not ready, will proceed without historical thresholds"
                )
                Shell.check(
                    f"touch {perf_wd}/historical-thresholds.tsv", verbose=True
                )
                return True
            result = cidb.do_select_query(
                query=GET_HISTORICAL_TRESHOLDS_QUERY, timeout=10, retries=3
            )
            if result is None:
                print(
                    "WARNING: Failed to fetch historical thresholds, will proceed without them"
                )
                Shell.check(
                    f"touch {perf_wd}/historical-thresholds.tsv", verbose=True
                )
                return True
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
            Shell.check(f"mkdir -p {db_path}/data/default/", verbose=True)
            dataset_paths = {
                "hits10": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_10m_single.tar",
                "hits100": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_100m_single.tar",
                "hits1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
                "values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
                "tpch10": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
                "tpcds1": "https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar",
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
                    status=Result.Status.OK if res else Result.Status.ERROR,
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
            # May slow down the server
            f"rm {perf_right_config}/config.d/memory_profiler.yaml ||:",
            f"rm {perf_right_config}/config.d/serverwide_trace_collector.xml ||:",
            f"rm {perf_right_config}/config.d/jemalloc_flush_profile.yaml ||:",
            f"rm -vf {perf_right_config}/config.d/keeper_max_request_size.xml",
            # backups disk uses absolute path, and this overlaps between servers, that could lead to errors
            f"rm {perf_right_config}/config.d/backups.xml ||:",
            # SSH config tries to bind a port not overridden per-server and may be unsupported by the reference binary
            f"rm {perf_right_config}/config.d/ssh.xml ||:",
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
            # Symlink user_files from the repository into both servers' user_files directories
            f'for f in ./tests/performance/user_files/*; do [ -e "$f" ] || continue; ln -sf "$(readlink -f "$f")" {perf_left}/db/user_files/; ln -sf "$(readlink -f "$f")" {perf_right}/db/user_files/; done',
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
            # Run 10 random queries per test by default, but all queries for benchmarks
            benchmarks = {"clickbench.xml", "tpch.xml", "tpcds.xml"}
            for test in test_files:
                max_queries = 0 if test in benchmarks else 10
                CHServer.run_test(
                    "./tests/performance/" + test,
                    runs=7,
                    max_queries=max_queries,
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

    if res and not info.is_local_run and JobStages.REPORT in stages:

        def insert_raw_query_metrics_data():
            cidb = CIDBCluster()
            assert cidb.is_ready()

            if not build_raw_query_metrics_tsv():
                print("WARNING: Failed to prepare raw query metrics TSV")
                return True

            check_start_timestamp = os.environ.get("CHPC_CHECK_START_TIMESTAMP", "")
            if check_start_timestamp:
                check_start_time = datetime.fromtimestamp(
                    int(check_start_timestamp)
                ).isoformat(sep=" ").split(".")[0]
            else:
                check_start_time = datetime.now().isoformat(sep=" ").split(".")[0]

            now = datetime.now()
            date = now.date().isoformat()

            with open(raw_query_metrics_path, "r", encoding="utf-8") as f:
                data = f.read()
            line_count = data.count("\n")

            insert_metadata = get_insert_metadata(info, compare_against_release)
            query = INSERT_RAW_QUERY_METRICS_DATA.format(
                RAW_QUERY_METRICS_TABLE=RAW_QUERY_METRICS_TABLE,
                EVENT_DATE=date,
                CHECK_START_TIME=check_start_time,
                PR_NUMBER=info.pr_number,
                REF_SHA=escape_sql_string(reference_sha),
                CUR_SHA=escape_sql_string(info.sha),
                **insert_metadata,
            )

            print(f"Do insert raw query metrics query: >>>\n{query}\n<<<")
            insert_ok = cidb.do_insert_query(
                query=query,
                data=data,
                timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                retries=3,
            )
            if insert_ok:
                print(f"Inserted [{line_count}] raw query metric lines")
            else:
                print(f"Inserted [{line_count}] raw query metric lines - failed")
            return True

        results.append(
            Result.from_commands_run(
                name="Insert raw query metrics data",
                command=insert_raw_query_metrics_data,
                with_info=True,
            )
        )

    if (
        res
        and not info.is_local_run
        and not compare_against_release
        and JobStages.REPORT in stages
    ):

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

            insert_metadata = get_insert_metadata(info, compare_against_release)
            query = INSERT_HISTORICAL_DATA.format(
                EVENT_DATE=date,
                EVENT_DATE_TIME=date_time,
                PR_NUMBER=info.pr_number,
                REF_SHA=escape_sql_string(reference_sha),
                CUR_SHA=escape_sql_string(info.sha),
                **insert_metadata,
            )

            print(f"Do insert historical data query: >>>\n{query}\n<<<")
            insert_ok = cidb.do_insert_query(
                query=query,
                data=data,
                timeout=Settings.CI_DB_INSERT_TIMEOUT_SEC,
                retries=3,
            )
            if insert_ok:
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

    if res and not info.is_local_run and JobStages.REPORT in stages:

        def insert_report_aggregates():
            """Upload all aggregate report TSVs and the tested-commits summary.

            Each upload is attempted independently; a failure or missing
            input for one table does not block the others. A single upload
            error does not fail the job either - these tables are purely
            informational for the UI, the source TSVs are still shipped in
            logs.tar.zst.
            """
            cidb = CIDBCluster()
            if not cidb.is_ready():
                print("WARNING: CIDB not ready - skipping report aggregate uploads")
                return True

            for cfg in REPORT_UPLOADS:
                try:
                    run_report_upload(
                        cfg=cfg,
                        cidb=cidb,
                        info=info,
                        reference_sha=reference_sha,
                        compare_against_release=compare_against_release,
                    )
                except Exception:
                    traceback.print_exc()

            try:
                insert_flamegraph_stacks(
                    cidb=cidb,
                    info=info,
                    reference_sha=reference_sha,
                    compare_against_release=compare_against_release,
                )
            except Exception:
                traceback.print_exc()

            return True

        results.append(
            Result.from_commands_run(
                name="Insert report aggregates",
                command=insert_report_aggregates,
                with_info=True,
            )
        )

    # TODO: code to fetch status was taken from old script as is - status is to be correctly set in Test stage and this stage is to be removed!
    message = ""
    if res and JobStages.CHECK_RESULTS in stages:

        def too_many_slow(msg):
            match = re.search(r"(|.* )(\d+) slower.*", msg)
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
            status = Result.Status.OK
            if "errors" in message.lower() or too_many_slow(message.lower()):
                status = Result.Status.FAIL
            # TODO: Remove until here
        except Exception:
            traceback.print_exc()
            status = Result.Status.FAIL
            message = "Failed to parse the report."

        if not status:
            status = Result.Status.FAIL
            message = "No status in report."
        elif not message:
            status = Result.Status.FAIL
            message = "No message in report."
        # Copy slower/unstable queries into Check Results so that Praktika
        # attaches per-query CIDB history links in the report.
        check_sub_results = []
        # Find the "Tests" sub-result that holds per-query results
        tests_result = None
        for r in results:
            if r.name == "Tests" and r.results:
                tests_result = r
                break
        if tests_result:
            # Always use master_head runs for the history query — they are
            # the stable baseline.  The CIDB check_name looks like
            # "Performance Comparison (arm_release, master_head, 1/6)".
            arch = get_perf_arch()
            check_name_pattern = f"%Performance%{arch}%master_head%"
            for tr in tests_result.results:
                if tr.status in ("slower", "unstable"):
                    sub = Result(
                        name=tr.name,
                        status=Result.Status.FAIL,
                        info=tr.status,
                        duration=tr.duration,
                    )
                    sub.set_label(
                        "query history",
                        link=build_perf_query_history_link(
                            tr.name, check_name_pattern
                        ),
                        hint="Performance history for this query on master",
                    )
                    check_sub_results.append(sub)

        results.append(
            Result(
                name="Check Results",
                status=status,
                info=message,
                duration=sw.duration,
                results=check_sub_results,
            )
        )

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
