import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import traceback
import urllib.parse
from datetime import datetime
from pathlib import Path
from threading import Thread

import yaml

from ci.jobs.scripts import log_export
from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.dataset_download import download_and_extract_datasets
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

# Settings for the report-building clickhouse-local (post-processing, not the
# measured servers). Keep in sync with CHPC_REPORT_LOCAL_{QUERY,SERVER}_SETTINGS
# in compare.sh.
REPORT_LOCAL_QUERY_SETTINGS = [
    # Keep report aggregations in RAM: report/tmp cannot hold a spill of the
    # heaviest randomization queries, so spilling only fails with NOT_ENOUGH_SPACE.
    "--max_bytes_before_external_group_by=0",
    "--max_bytes_ratio_before_external_group_by=0",
    "--max_bytes_before_external_sort=0",
    "--max_bytes_ratio_before_external_sort=0",
]
REPORT_LOCAL_SERVER_SETTINGS = [
    # Track each process against its own RSS, not the job cgroup (MEMORY_LIMIT_EXCEEDED).
    "--",
    "--memory_worker_use_cgroup=0",
]

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
     query_display_name String,
     changed_threshold Float64,
     unstable_threshold Float64'
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
        "SELECT\n" + select_all + "\n"
        "FROM input('" + input_schema + "')\n"
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
    EXPORT_LOGS = "export_logs"
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


def cpu_pinning_enabled():
    """Pinning requires Linux (taskset, sysfs topology, sched_getaffinity),
    not just the CPU family: a local x86_64 macOS run must not get a taskset
    prefix it cannot execute."""
    return Utils.is_amd() and os.uname().sysname == "Linux"


def get_physical_core_cpu_list():
    """Return a taskset -c CPU list with one hyperthread per physical core.

    On the x86_64 perf runner (m7i.4xlarge: 8 physical cores x 2 hyperthreads)
    both measured servers are pinned to this list so that query threads never
    end up sharing a hyperthread sibling with each other depending on scheduler
    mood - a top suspect for the amd-vs-arm A/A noise gap (0.51% vs 0.42%).

    Parses /sys/devices/system/cpu/cpu*/topology/thread_siblings_list, keeps
    the first ALLOWED sibling of each unique pair (intersected with the
    process affinity mask), and falls back to all allowed cpus if the sysfs
    topology is unavailable. Only call at runtime on the Linux CI host (there
    is no /sys on macOS) - never at import time.

    Must stay in sync with pinned_cpu_list in ci/jobs/scripts/perf/compare.sh
    (the server restart path used by the standalone flows and the
    confirm-changes rerun).
    """
    # Sysfs exposes the HOST topology: on a cpuset-limited run the process may
    # only be allowed a subset of it, and taskset with a disallowed CPU fails
    # to start the servers. Pick, per physical core, the first sibling the
    # process is actually allowed to run on.
    getaffinity = getattr(os, "sched_getaffinity", None)
    try:
        allowed = getaffinity(0) if getaffinity else None
    except OSError:
        allowed = None
    cores = {}
    # Per-file tolerance, matching the compare.sh copy: one unreadable or
    # malformed sibling file must not discard the rest of the topology, or
    # the two pinners could pin the main run and the confirm rerun to
    # different CPU sets.
    for path in Path("/sys/devices/system/cpu").glob(
        "cpu[0-9]*/topology/thread_siblings_list"
    ):
        # Formats seen in the wild: "0,8", "0-1", "0" (no SMT).
        try:
            siblings = [
                int(s) for s in re.split(r"[,-]", path.read_text().strip()) if s
            ]
        except (OSError, ValueError):
            continue
        usable = [c for c in siblings if allowed is None or c in allowed]
        if usable:
            cores[min(siblings)] = min(usable)
    cpus = set(cores.values())
    if not cpus:
        # Without topology, halving would be a guess that drops real cores on
        # non-SMT hosts and on masks that already expose one sibling per core
        # (e.g. Cpus_allowed_list: 1,3). Keep every allowed CPU instead: the
        # degraded mode allows hyperthread sharing (the pre-pinning behavior)
        # but never skews measurements by idling half the cores.
        print(
            "WARNING: could not parse cpu topology from sysfs; using all "
            "allowed cpus (sibling pairs unknown, hyperthread sharing possible)"
        )
        if allowed:
            cpus = set(allowed)
        else:
            cpus = set(range(os.cpu_count() or 2))
    return ",".join(str(cpu) for cpu in sorted(cpus))


# users.d override applied only on x86_64, where both servers are pinned with
# taskset to one hyperthread per physical core (see get_physical_core_cpu_list).
# The static default (max_threads=12, tests/performance/scripts/config/users.d/
# perf-comparison-tweaks-users.xml) is kept for arm (m8g.4xlarge: 16 real
# cores). The zzz- prefix makes this file sort after (and thus override) the
# static users.d files. Standalone compare.sh entrypoints write the same
# override in write_max_threads_override (keep the two in sync).
MAX_THREADS_OVERRIDE_FILE = "zzz-cpu-pinning-max-threads.xml"
MAX_THREADS_OVERRIDE_XML = """\
<!--
    Written by ci/jobs/performance_tests.py at job setup, x86_64 only (arm
    keeps max_threads=12 from perf-comparison-tweaks-users.xml).

    Both servers are pinned with taskset to one hyperthread per physical core
    and max_threads is set to the size of that CPU set (e.g. 8 on
    m7i.4xlarge: 8 physical cores x 2 hyperthreads), one query thread per
    pinned CPU, so whether two threads share a hyperthread sibling no longer
    depends on the scheduler (measured A/A noise: amd 0.51% vs arm 0.42%).
-->
<clickhouse>
    <profiles>
        <default>
            <max_threads>{max_threads}</max_threads>
        </default>
    </profiles>
</clickhouse>
"""


def write_max_threads_override():
    """Write the x86_64 max_threads override into both servers' users.d.

    max_threads is derived from the pinned CPU list, so the one-thread-per-
    pinned-CPU invariant holds on any runner shape (smaller/larger/non-SMT
    x86 hosts), not just the current m7i.4xlarge.
    """
    if not cpu_pinning_enabled():
        print("CPU pinning disabled (needs Linux x86_64) - keeping the static max_threads")
        # Reused workspaces (mkdir -p / cp -r) may carry an override from an
        # earlier x86_64 run; remove it so the static value actually applies.
        for config_dir in (perf_left_config, perf_right_config):
            stale = Path(config_dir) / "users.d" / MAX_THREADS_OVERRIDE_FILE
            if stale.exists():
                print(f"Removing stale max_threads override [{stale}]")
                stale.unlink()
        return True
    max_threads = len(get_physical_core_cpu_list().split(","))
    for config_dir in (perf_left_config, perf_right_config):
        target = Path(config_dir) / "users.d" / MAX_THREADS_OVERRIDE_FILE
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(MAX_THREADS_OVERRIDE_XML.format(max_threads=max_threads))
        print(f"Wrote max_threads={max_threads} override to [{target}]")
    return True


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


def build_check_results_children(tests_result, check_name_pattern):
    """Per-query rows for "Check Results": one row per slower/unstable query.

    Rows carry compare.sh's verdict, which the report renderer classifies
    natively. They cannot affect the job status: the caller passes "Check
    Results" an explicit status, and `Result.create_from` aggregates children
    only when no status is given.
    """
    # compare.sh emits a row per side, but a truncated ci-checks.tsv can leave a
    # query with either side alone, so group by query name and represent each
    # group by its candidate side when it survived.
    side_priority = {"::new": 0, "": 1, "::old": 2}
    chosen = {}
    for tr in tests_result.results:
        if tr.status not in ("slower", "unstable"):
            continue
        side = next((s for s in ("::new", "::old") if tr.name.endswith(s)), "")
        base = tr.name[: len(tr.name) - len(side)]
        previous = chosen.get(base)
        if previous is None or side_priority[side] < side_priority[previous[0]]:
            chosen[base] = (side, tr)

    children = []
    for base, (_, tr) in chosen.items():
        sub = Result(name=base, status=tr.status, duration=tr.duration)
        sub.set_label(
            "query history",
            # The represented row's own name: CIDB's test_name keeps the side
            # suffix, and the link filters on an exact match.
            link=build_perf_query_history_link(tr.name, check_name_pattern),
            hint="Performance history for this query on master",
        )
        children.append(sub)
    return children


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
        ["clickhouse-local", "--query", BUILD_RAW_QUERY_METRICS_QUERY, *REPORT_LOCAL_QUERY_SETTINGS, *REPORT_LOCAL_SERVER_SETTINGS],
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
        ["clickhouse-local", "--query", BUILD_FLAMEGRAPH_UPLOAD_QUERY, *REPORT_LOCAL_QUERY_SETTINGS, *REPORT_LOCAL_SERVER_SETTINGS],
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


# --- Export of the system logs to the CI Logs cluster ----------------------
# Both servers export their `system.*_log` tables the way every other check
# does it: a materialized view per log table pushes the rows into a
# `Distributed` table, which sends them to the CI Logs cluster in the
# background (see ci/jobs/scripts/log_export.py). The only difference is that
# the sends are held back while the queries are measured - writing the rows
# into the local files of the `Distributed` tables is cheap, sending them over
# the network is not - and everything accumulated is sent after the last test,
# see export_system_logs below.
#
# Both servers write into the same destination tables and the extra columns
# have no field for the server, so the two are told apart by a suffix in
# `check_name`: '<job name> (left)' is the reference build and
# '<job name> (right)' is the patched one.

# `system.build_options` reports the full git hash of the build; it is empty
# only for a build made without git information available (cmake/git.cmake).
GIT_HASH_RE = re.compile(r"[0-9a-f]{7,40}")


def get_server_commit_sha(server):
    """The commit of the build a server runs, or an empty string if it cannot
    be read."""
    sha = (
        server.ask("SELECT value FROM system.build_options WHERE name = 'GIT_HASH'")
        or ""
    ).strip()
    return sha if GIT_HASH_RE.fullmatch(sha) else ""


def write_ci_logs_sender_user(config_dir, binary):
    """Write the `ci_logs_sender` user, which the export views run as, into a
    server's users.d - without the settings that build does not know.

    A setting a build does not know is not ignored in a profile: the server
    refuses to start with `Setting ... is neither a builtin setting nor ...`.
    The reference server runs an older build, so the first setting added to the
    shared file would otherwise take the whole check down with it (the same
    reason the job strips new settings from `keeper_port.xml`).
    """
    known_settings = set(
        Shell.get_output(
            f'{binary} local --query "SELECT name FROM system.settings"', strict=True
        ).split()
    )
    config = yaml.safe_load(Path(log_export.CI_LOGS_SENDER_USER_CONFIG).read_text())
    profile = config["profiles"]["ci_logs_sender"]
    constraints = profile.get("constraints", {})
    unknown = sorted(
        {
            name
            for name in list(profile) + list(constraints)
            if name != "constraints" and name not in known_settings
        }
    )
    if unknown:
        print(
            f"WARNING: The build of [{binary}] does not know {unknown} - "
            "dropping them from the ci_logs_sender profile"
        )
        for name in unknown:
            profile.pop(name, None)
            constraints.pop(name, None)
    target = Path(config_dir) / "users.d" / "ci_logs_sender.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        f"# Generated from {log_export.CI_LOGS_SENDER_USER_CONFIG}\n"
        + yaml.dump(config, default_flow_style=False)
    )
    return True


def create_log_export_configs():
    """Add the CI Logs cluster, and the user the export views run as, to the
    config of both servers. Must run before they are started."""
    if Info().is_local_run:
        print("Local run - the system logs will not be exported")
        return True
    try:
        host, password = log_export.get_credentials()
        for config_dir, binary in (
            (perf_left_config, f"{perf_left}/clickhouse"),
            (perf_right_config, f"{perf_right}/clickhouse"),
        ):
            if not log_export.create_config(config_dir, host, password):
                print(f"WARNING: Failed to write the log export config into [{config_dir}]")
                continue
            write_ci_logs_sender_user(config_dir, binary)
    except Exception:
        # Best effort: a job that cannot export its system logs still runs.
        traceback.print_exc()
    return True


def start_log_export(servers):
    """Create the export views on both servers and hold the sends back until
    the tests are over.

    The reference server is skipped when the commit of its build cannot be
    read: `commit_sha` is what attributes its rows to a build, and the rows
    would be of no use in the CI Logs cluster without it.

    A server whose export cannot be held back has it torn down instead: the
    export costs this job its logs when it fails, never its measurements.
    """
    info = Info()
    try:
        host, password = log_export.get_credentials()
    except Exception:
        traceback.print_exc()
        print("WARNING: No CI Logs cluster credentials, the system logs will not be exported")
        return True
    check_start_time = int(os.environ["CHPC_CHECK_START_TIMESTAMP"])
    for node_name, server in servers:
        # The patched server runs the build of the commit this check reports
        # for, like every other check; the reference server runs an older
        # build and names its own commit.
        if server.is_left:
            commit_sha = get_server_commit_sha(server)
            if not commit_sha:
                print(
                    "WARNING: Cannot read the build commit of the reference server, "
                    "its system logs will not be exported"
                )
                continue
        else:
            commit_sha = info.sha
        try:
            if not log_export.start(
                check_start_time,
                host=host,
                password=password,
                port=server.port,
                check_name_suffix=f" ({node_name})",
                commit_sha=commit_sha,
            ):
                print(
                    f"WARNING: Failed to set up the system log export on the [{node_name}] server"
                )
                continue
            # After the setup, which is what creates the tables the sends are
            # stopped for.
            if not log_export.stop_distributed_sends(server.port):
                # Fail closed. The measured numbers are what this job is for,
                # the logs are a by-product: an export that cannot be held
                # back has to go, or it would ship rows over the network while
                # the queries are measured.
                print(
                    f"WARNING: Cannot hold back the log export of the [{node_name}] server "
                    "for the measured window - tearing the export down, "
                    "this server will not export its system logs"
                )
                log_export.stop(server.port)
        except Exception:
            traceback.print_exc()
            # The same, for a failure raised rather than reported: whatever the
            # export is in the middle of, it must not outlive this stage.
            try:
                log_export.stop(server.port)
            except Exception:
                traceback.print_exc()
    return True


def export_system_logs(servers):
    """Send what the servers accumulated while the tests were running to the
    CI Logs cluster, and drop the export views.

    Best effort throughout: the measurements are over by now, and a server
    whose export was torn down in `start_log_export` has nothing left to send.
    The `SYSTEM FLUSH DISTRIBUTED` of `log_export.stop` sends the accumulated
    files whatever came of the `START` above (`processFiles(force = true)` does
    not consult the send lock), so the rows are not lost if it did not take.
    """
    for node_name, server in servers:
        print(f"Export the system logs of the [{node_name}] server")
        try:
            log_export.start_distributed_sends(server.port)
            log_export.stop(server.port)
        except Exception:
            traceback.print_exc()
    return True


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


def match_reference_debug_info():
    # addressToLine resolves a frame to "file:line" only where DWARF covers
    # ClickHouse code. PR builds use -g0 (DISABLE_ALL_DEBUG_SYMBOLS): the symbol
    # table remains (addressToSymbol works) but there is no line info, so the
    # patched binary symbolizes differently from the reference (master) build and
    # flamegraph tooling cannot match the frames. A ".debug_info" section is not a
    # reliable signal (Rust crates emit one even under -g0), so probe how many
    # system.stack_trace frames resolve to a line on each binary and strip the
    # reference only when the patched binary resolves far fewer. Merge-to-master
    # resolves comparably on both and is left untouched. Must match
    # compare.sh::match_reference_debug_info.
    left = Shell.get_output(f"readlink -f {perf_left}/clickhouse-server", strict=True)
    right = Shell.get_output(f"readlink -f {perf_right}/clickhouse-server", strict=True)
    probe = (
        "select countIf(addressToLine(arrayJoin(trace)) like '%:%') "
        "from system.stack_trace"
    )

    def resolved_lines(binary):
        # Running clickhouse also decompresses the self-extracting binary in place.
        out = Shell.get_output(
            f'{binary} local --allow_introspection_functions=1 --query "{probe}"'
        )
        return int(out) if out and out.strip().isdigit() else 0

    if resolved_lines(right) * 4 < resolved_lines(left):
        Shell.check(f"strip --strip-debug {left}", verbose=True)
    else:
        print("Patched binary has comparable line info, leaving reference as-is")


class CHServer:
    # upstream/master
    LEFT_SERVER_PORT = 9001
    LEFT_SERVER_KEEPER_PORT = 9181
    LEFT_SERVER_KEEPER_RAFT_PORT = 9234
    LEFT_SERVER_INTERSERVER_PORT = 9009
    LEFT_SERVER_HTTP_PORT = 8123
    # patched version
    RIGHT_SERVER_PORT = 19001
    RIGHT_SERVER_KEEPER_PORT = 19181
    RIGHT_SERVER_KEEPER_RAFT_PORT = 19234
    RIGHT_SERVER_INTERSERVER_PORT = 19009
    RIGHT_SERVER_HTTP_PORT = 18123

    # lg2 of the average byte interval between jemalloc allocation samples.
    # Denser than the 512 KiB (19) default: we profile single queries in
    # isolation, so the profile needs to be dense to yield useful
    # JemallocSample flamegraphs. Must match compare.sh.
    JEMALLOC_PROFILER_SAMPLING_RATE = 16

    def __init__(self, is_left=False):
        if is_left:
            server_port = self.LEFT_SERVER_PORT
            keeper_port = self.LEFT_SERVER_KEEPER_PORT
            raft_port = self.LEFT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.LEFT_SERVER_INTERSERVER_PORT
            http_port = self.LEFT_SERVER_HTTP_PORT
            serever_path = f"{temp_dir}/perf_wd/left"
            log_file = f"{serever_path}/server.log"
        else:
            server_port = self.RIGHT_SERVER_PORT
            keeper_port = self.RIGHT_SERVER_KEEPER_PORT
            raft_port = self.RIGHT_SERVER_KEEPER_RAFT_PORT
            inter_server_port = self.RIGHT_SERVER_INTERSERVER_PORT
            http_port = self.RIGHT_SERVER_HTTP_PORT
            serever_path = f"{temp_dir}/perf_wd/right"
            log_file = f"{serever_path}/server.log"

        self.log_fd = None
        self.log_file = log_file
        self.port = server_port
        self.server_path = serever_path
        self.is_left = is_left
        self.name = "Reference" if is_left else "Patched"

        # On x86_64 pin both servers to one hyperthread per physical core (the
        # same list for both: they are measured alternately, not concurrently).
        # Together with the max_threads=8 users.d override this keeps one query
        # thread per physical core and removes scheduler-dependent hyperthread
        # sibling sharing. arm (real cores only) is unchanged.
        taskset_prefix = (
            f"taskset -c {get_physical_core_cpu_list()} "
            if cpu_pinning_enabled()
            else ""
        )

        # The perf-comparison config removes <http_port>; re-enable it on the
        # command line (a documented config override, see Server.cpp) with a
        # distinct port per server, so that shell-script tests can talk to the
        # server over HTTP.
        self.start_cmd = f"{taskset_prefix}{serever_path}/clickhouse-server --config-file={serever_path}/config/config.xml \
            -- --path {serever_path}/db --user_files_path {serever_path}/db/user_files \
            --top_level_domains_path {serever_path}/top_level_domains --tcp_port {server_port} \
            --http_port {http_port} \
            --keeper_server.tcp_port {keeper_port} --keeper_server.raft_configuration.server.port {raft_port} \
            --keeper_server.storage_path {serever_path}/coordination --zookeeper.node.port {keeper_port} \
            --interserver_http_port {inter_server_port} \
            --jemalloc_profiler_sampling_rate {self.JEMALLOC_PROFILER_SAMPLING_RATE}"

    def start(self):
        print(f"Starting [{self.name}] ClickHouse server")
        # Rewrite the max_threads override right before starting: praktika
        # stages can be re-entered by a fresh process whose affinity mask may
        # differ from the one CONFIGURE saw, and taskset (start_cmd) is
        # computed at construction time - the override must match it.
        # Idempotent; compare.sh::restart does the same for its flows.
        write_max_threads_override()
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
        print("ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print("ClickHouse server ready")
        else:
            print("ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --receive_timeout=5 --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print("Server not ready, wait")
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
        cls, test_file, runs=None, max_queries=0, pr_number=0, results_path=f"{temp_dir}/perf_wd/"
    ):
        test_name = test_file.split("/")[-1].removesuffix(".xml")
        sw = Utils.Stopwatch()
        # --runs ("at least N runs per query") is passed only when explicitly
        # requested; by default the adaptive run policy decides the counts.
        runs_arg = f"--runs {runs}" if runs is not None else ""
        res, out, err = Shell.get_res_stdout_stderr(
            f"./tests/performance/scripts/perf.py --host localhost localhost \
                --port {cls.LEFT_SERVER_PORT} {cls.RIGHT_SERVER_PORT} \
                --binary {perf_left}/clickhouse {perf_right}/clickhouse \
                --http-port {cls.LEFT_SERVER_HTTP_PORT} {cls.RIGHT_SERVER_HTTP_PORT} \
                {runs_arg} --max-queries {max_queries} \
                --profile-seconds 10 \
                --pr-number {pr_number} \
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


# The number of distinct "slower" queries that fails the whole performance
# check in the commit-to-commit (`master_head`) mode. This is the gate that
# actually decides the Praktika `Check Results` status: `report.py` embeds a
# status into `report.html`, but `main` below discards it ("always green mode")
# and recomputes the final status by reparsing the "N slower" message, so the
# effective gate lives here. The value must stay synchronized with the
# slower-queries threshold in `ci/jobs/scripts/perf/report.py`. It is
# intentionally high: a handful of "slower" queries is dominated by CI noise (a
# single bad shard run, frequency scaling, or code-layout artifacts can push
# several unrelated micro benchmarks over their per-query thresholds at once),
# while a genuine regression shows up as a small cluster of related queries
# with large magnitudes that the per-query thresholds catch on their own.
SLOWER_QUERIES_FAIL_THRESHOLD = 10

# The gate for the cumulative `release_base` mode. That comparison accumulates
# every performance change since the release branch point, so an absolute
# slower-count gate inevitably drifts into permanent red: once master collects
# more than SLOWER_QUERIES_FAIL_THRESHOLD slower queries in one shard, the
# check fails on every commit until the next release resets the baseline, and
# the status stops pointing at any specific commit. Instead, fail only when
# the slower count grew by more than this delta compared to the previous
# master run of the same job - red then blames the commit that introduced the
# regression and recovers on the next commit by itself. Measured on 10 days of
# master runs: run-to-run count-delta noise is p90 <= 3 per shard, while real
# regression landings showed +8..+16 (e.g. the `direct_dictionary` regression,
# https://github.com/ClickHouse/ClickHouse/issues/115803).
SLOWER_QUERIES_DELTA_FAIL_THRESHOLD = 5


def parse_slower_count(message):
    match = re.search(r"(|.* )(\d+) slower.*", message)
    return int(match.group(2).strip()) if match else 0


# A genuine perf summary produced by `report.py` is either "See the report"
# (nothing notable) or a comma-separated list of "<N> too long", "<N> faster",
# "<N> slower", "<N> unstable" phrases. "<N> errors" is deliberately excluded:
# a run with errors may have skipped queries, so its slower count would
# understate the baseline. Everything else - "No status in report.",
# "No message in report.", "Failed to parse the report.", "Errors while
# building the report." - is a failure sentinel, not a summary.
_PERF_SUMMARY_PART_RE = re.compile(r"^\d+ (too long|faster|slower|unstable)$")


def is_perf_summary_message(message):
    """Return True when a lowercased result message is a normal perf summary
    that can serve as a slower-count baseline."""
    # A previous run of this job appends "; release base ...; delta vs prev
    # master run (...)" to its own summary - ignore everything after the
    # first ";".
    summary = message.split(";")[0].strip()
    if summary == "see the report":
        return True
    parts = [p.strip() for p in summary.split(",")]
    return all(_PERF_SUMMARY_PART_RE.match(p) for p in parts)


# The marker a `release_base` run appends to its own result message so that the
# next master run can tell which release baseline the count was measured
# against. `release_base` is cumulative since the release branch point, so the
# counts of two runs are comparable only when both used the same baseline.
_RELEASE_BASE_MARKER = "release base"
_RELEASE_BASE_RE = re.compile(rf"{_RELEASE_BASE_MARKER} ([0-9a-f]+)")


def format_release_base_marker(release_base_sha):
    return f"; {_RELEASE_BASE_MARKER} {release_base_sha[:12]}"


def parse_release_base(message):
    """Return the release baseline sha recorded in a lowercased result message,
    or None when the message carries no marker (a run that predates it)."""
    match = _RELEASE_BASE_RE.search(message)
    return match.group(1) if match else None


def too_many_slow(message):
    return parse_slower_count(message) > SLOWER_QUERIES_FAIL_THRESHOLD


# Outcomes of fetching one previous result artifact from S3.
FETCH_OK = "ok"
FETCH_MISSING = "missing"
FETCH_ERROR = "error"


def fetch_prev_master_result(link):
    """Fetch a previous run's `result_*.json` from S3.

    Returns `(FETCH_OK, body)` for an existing object, `(FETCH_MISSING, None)`
    when the object does not exist, and `(FETCH_ERROR, None)` for a transport
    failure. The distinction matters: a missing object usually means the commit
    never ran this job (see `classify_missing_prev_master_run`), while a timeout
    or a TLS error says nothing about the commit, and silently treating it as a
    miss would compare the current run against an older baseline.

    Note that the bucket denies `s3:ListBucket`, so S3 answers `403` instead of
    `404` for a key that does not exist - both codes mean "missing" here."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        body_file = Path(tmp_dir) / "result.json"
        http_code = Shell.get_output(
            f"curl -s --compressed --max-time 60 -o {body_file} "
            f'-w "%{{http_code}}" "{link}"'
        ).strip()
        # `Shell.get_output` returns an empty string when curl exits non-zero,
        # and curl reports "000" when no response was received at all.
        if http_code in ("", "000"):
            print(f"WARNING: transport failure while fetching [{link}]")
            return FETCH_ERROR, None
        if http_code in ("403", "404"):
            return FETCH_MISSING, None
        if http_code != "200":
            print(f"WARNING: unexpected HTTP {http_code} for [{link}]")
            return FETCH_ERROR, None
        return FETCH_OK, body_file.read_text(encoding="utf-8")


# Why one commit's missing `result_*.json` is skipped and another one's stops
# the walk. `MASTER_RUN_NEVER_SCHEDULED` means this job provably never started
# at that commit, so the commit carries no measurement to miss and the walk may
# continue to an older one. `MASTER_RUN_INCOMPLETE` means it did start (or was
# scheduled) and simply has no result to read: continuing past it would compute
# the delta across that commit's changes too, which is exactly the stale-baseline
# attribution the delta gate exists to avoid.
MASTER_RUN_NEVER_SCHEDULED = "never_scheduled"
MASTER_RUN_INCOMPLETE = "incomplete"

# The workflow-level praktika report of a master run, which lists every job of
# that run with its status. `check_ci.py` reads the same object.
MASTER_WORKFLOW_RESULT_FILE = "result_masterci.json"


def classify_missing_prev_master_run(job_name, sha):
    """Tell whether this job was ever scheduled at master commit `sha`, given
    that its `result_*.json` is missing there.

    A missing result is the common case rather than an anomaly, and it is not a
    symptom of a broken run:

      * `MasterCI` is a push-triggered workflow, and master commits land
        seconds apart, so a single push event covers a batch of commits and the
        workflow runs on the head of the batch only. The other commits of the
        batch have no `MasterCI` run at all, hence no report object either.
      * `MasterCI` sets `enable_job_filtering_by_changes`, so even when it does
        run, a commit that touches nothing the perf job depends on gets it
        `SKIPPED` ("Not affected by the changed files and not required").

    Both are `MASTER_RUN_NEVER_SCHEDULED`: no measurement was taken and none
    ever will be, so the walk has to look further back or the delta gate would
    be unusable on the majority of master runs.

    Everything else is `MASTER_RUN_INCOMPLETE`: the job is `PENDING`/`RUNNING`
    and its result is still to come, it was `DROPPED`, it reached a terminal
    status without publishing a result, or the report itself cannot be fetched
    or parsed. Those all stop the walk, and the caller falls back to the
    absolute gate for this one run - the next master run finds this run's own
    result and gets its delta back."""
    link = (
        "https://s3.amazonaws.com/clickhouse-test-reports/REFs/master/"
        f"{sha}/{MASTER_WORKFLOW_RESULT_FILE}"
    )
    state, out = fetch_prev_master_result(link)
    if state == FETCH_MISSING:
        print(f"INFO: master commit {sha} has no {MASTER_WORKFLOW_RESULT_FILE}")
        return MASTER_RUN_NEVER_SCHEDULED
    if state == FETCH_ERROR:
        print(f"WARNING: failed to fetch the master workflow report [{link}]")
        return MASTER_RUN_INCOMPLETE
    try:
        jobs = json.loads(out).get("results") or []
    except Exception:
        print(f"WARNING: failed to parse the master workflow report [{link}]")
        return MASTER_RUN_INCOMPLETE
    for job in jobs:
        if not isinstance(job, dict) or job.get("name") != job_name:
            continue
        status = job.get("status")
        if status == Result.Status.SKIPPED:
            print(f"INFO: [{job_name}] was skipped at master commit {sha}")
            return MASTER_RUN_NEVER_SCHEDULED
        print(
            f"WARNING: [{job_name}] at master commit {sha} is {status} but "
            "published no result"
        )
        return MASTER_RUN_INCOMPLETE
    # The job is absent from the report: this master run did not schedule it.
    print(f"INFO: [{job_name}] is not part of the master run of {sha}")
    return MASTER_RUN_NEVER_SCHEDULED


def find_prev_master_slower_count(job_name, commits, release_base_sha):
    """Find the "slower" query count reported by the most recent valid run of
    this job on a predecessor master commit. Returns (count, sha), or
    (None, None) when no usable previous run is found and the caller has to
    fall back to the absolute gate.

    The walk stops - rather than skipping to an older commit - as soon as a
    predecessor is found whose result cannot be used as a baseline: a
    transport failure, a malformed body, a non-summary message (errors,
    sentinels like "No status in report."), a run measured against a different
    release baseline, or a run that was scheduled but has no result yet
    (`MASTER_RUN_INCOMPLETE`). Skipping such a commit would silently compare
    the current run against an older one, so red would no longer blame the
    commit that introduced the regression.

    `commits` on master runs is `master_track_commits_sha`, the first-parent
    chain of master recorded by the `store_data` hook. A missing result on that
    chain is not by itself a sign of a broken predecessor - most master commits
    never run this job at all - so each missing result is classified by
    `classify_missing_prev_master_run` and only a provably never-scheduled one
    is skipped. The walk has no cutoff: truncating the list could exhaust it
    before reaching the previous run that did measure this job."""
    result_file_name = f"result_{Utils.normalize_string(job_name)}.json"
    for sha in commits:
        link = f"https://s3.amazonaws.com/clickhouse-test-reports/REFs/master/{sha}/{result_file_name}"
        state, out = fetch_prev_master_result(link)
        if state == FETCH_MISSING:
            if classify_missing_prev_master_run(job_name, sha) == MASTER_RUN_INCOMPLETE:
                return None, None
            continue
        if state == FETCH_ERROR:
            return None, None
        try:
            prev_message = json.loads(out).get("info", "")
        except Exception:
            print(f"WARNING: failed to parse previous run result [{link}]")
            return None, None
        prev_message = prev_message.lower()
        if not prev_message or not is_perf_summary_message(prev_message):
            print(
                f"WARNING: previous run result for {sha} is not a usable perf "
                f"summary: {prev_message!r}"
            )
            return None, None
        prev_release_base = parse_release_base(prev_message)
        if not prev_release_base or not release_base_sha.startswith(prev_release_base):
            print(
                f"WARNING: previous run {sha} was measured against release "
                f"baseline {prev_release_base!r}, not {release_base_sha!r}"
            )
            return None, None
        return parse_slower_count(prev_message), sha
    return None, None


def read_ci_checks_results(path):
    """Parse `ci-checks.tsv` (TSVWithNamesAndTypes).

    Returns `(results, malformed, complete)`:
      - `results`: valid `Result` rows;
      - `malformed`: number of rows skipped because they were cut short;
      - `complete`: whether both header lines AND at least one data row were
        present, i.e. whether the file is worth importing at all. A file with no
        data row is necessarily truncated: compare.sh's `upload_results` unions
        an unconditional single-row summary select into every `ci-checks.tsv`,
        so a run that legitimately produced only the two header lines does not
        exist.

    Never raises. compare.sh writes this file last, so a failure there (most
    often a full disk) leaves an arbitrary byte prefix. Raising here would kill
    the job before praktika uploads the artifacts, which is the failure this
    parser exists to remove, so every shape of prefix is tolerated: a cut inside
    a multi-byte character (hence the lenient decode, as in
    `stress_job.read_test_results`), a cut header line (fewer field names than a
    data row has cells), and a cut data row (`csv.DictReader` fills the missing
    fields with `restval`, i.e. `None`).
    """
    results = []
    malformed = 0
    # Decode leniently: a byte prefix of a UTF-8 file can end inside a
    # multi-byte sequence, which a strict decode rejects.
    with open(path, "rb") as descriptor:
        content = descriptor.read().decode("utf-8", errors="replace")
    lines = content.split("\n")
    # A complete file is newline-terminated: compare.sh writes it through a
    # ClickHouse `File(TSVWithNamesAndTypes)` table, which terminates every row.
    # So a non-empty trailing fragment is a line cut mid-write, and nothing in
    # it can be trusted - not even the fields that happen to be present, since a
    # number cut after its first digits still parses.
    cut_line = lines.pop()
    # Column names, column types, and at least the summary row compare.sh always
    # emits: anything shorter is a prefix, not a completed run.
    if len(lines) < 3:
        return results, malformed, False
    if cut_line:
        malformed += 1
    header = lines[0].strip().split("\t")
    reader = csv.DictReader(lines[2:], delimiter="\t", fieldnames=header)
    for row in reader:
        name = row.get("test_name")
        if name == "":
            # The summary row carries the report message, not a test case.
            continue
        if (
            name is None
            or row.get("test_status") is None
            or row.get("test_duration_ms") is None
            # Require every column, not only the three consumed ones: a row
            # missing any field was cut short.
            or any(row.get(field) is None for field in header)
        ):
            malformed += 1
            continue
        try:
            duration = float(row["test_duration_ms"]) / 1000
        except (TypeError, ValueError):
            malformed += 1
            continue
        results.append(
            Result(name=name, status=row["test_status"], duration=duration)
        )
    return results, malformed, True


def import_ci_checks_results(path, results):
    """Import `ci-checks.tsv` rows into the previous subtask's results.

    Returns True when the file was importable. A file with no data row at all -
    empty, or only the header lines - is reported and left unimported. That
    distinction is a diagnostic one, not a data-preserving one: every subtask
    `main()` appends before this call is built without a `results=` argument, so
    the assignment target's row list is empty either way and there is nothing an
    empty assignment could destroy. A file that lost individual rows still
    imports the intact ones and reports how many it skipped, because degrading
    beats dying. An absent file is the atomic publish's own failure signal -
    `upload_results` deliberately leaves the final path missing when the write
    fails - so it must warn here rather than reach `open`, whose
    `FileNotFoundError` would escape `main()` and kill the job before praktika
    uploads the artifacts.
    """
    if not Path(path).is_file():
        print("WARNING: compare.sh did not generate ci-checks.tsv file")
        return False
    test_results, malformed, complete = read_ci_checks_results(path)
    if not complete:
        print("WARNING: ci-checks.tsv is empty or truncated - skipping test case import")
        return False
    if malformed:
        print(f"WARNING: ci-checks.tsv had {malformed} malformed row(s) - skipped")
    # results[-2] is a previuos subtask
    results[-2].results = test_results
    return True


def _perf_client(port):
    return (
        f"clickhouse-client --port {port} "
        "--max_memory_usage 30G --max_memory_usage_for_user 30G "
        "--max_estimated_execution_time 0 --max_execution_time 1800 --receive_timeout 1800"
    )


def rebuild_table(port, source, destination):
    # Re-insert an attached dataset through the running server so its parts are
    # written by that server's own binary and settings (sparse columns,
    # statistics, mark format) instead of the frozen tarball format, then
    # OPTIMIZE FINAL back to a single part matching the original layout. INSERT
    # is what recomputes serialization from the data; a bare OPTIMIZE would
    # inherit the source parts' serialization, so it cannot replace the insert.
    # For an in-place rebuild the fresh copy is built under a temporary name and
    # swapped in with RENAME (the datasets live in Ordinary databases, so
    # EXCHANGE TABLES is not available).
    client = _perf_client(port)
    if Shell.get_output(f'{client} --query "EXISTS TABLE {source}"').strip() != "1":
        # A missing source is only expected for the cross-name rebuild
        # (datasets.hits_v1 -> test.hits) retried after a previous run already
        # built the destination and dropped the source. Everywhere else a
        # missing source means the dataset failed to attach: fail closed, so the
        # completion marker is never written for a table that was not rebuilt.
        if source != destination and Shell.get_output(f'{client} --query "EXISTS TABLE {destination}"').strip() == "1":
            print(f"rebuild_table: {source} already consumed into {destination}, skipping")
            return
        raise RuntimeError(f"rebuild_table: source {source} is not attached")
    insert_settings = "enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
    target = f"{destination}_rebuild" if source == destination else destination
    # Drop any leftover target from an interrupted previous run before rebuilding.
    Shell.check(f'{client} --query "DROP TABLE IF EXISTS {target} SYNC"', strict=True, verbose=True)
    Shell.check(f'{client} --query "CREATE TABLE {target} AS {source}"', strict=True, verbose=True)
    Shell.check(f'{client} --query "INSERT INTO {target} SELECT * FROM {source} SETTINGS {insert_settings}"', strict=True, verbose=True)
    Shell.check(f'{client} --query "OPTIMIZE TABLE {target} FINAL"', strict=True, verbose=True)
    if target != destination:
        old = f"{destination}_old"
        Shell.check(f'{client} --query "DROP TABLE IF EXISTS {old} SYNC"', strict=True, verbose=True)
        Shell.check(f'{client} --query "RENAME TABLE {destination} TO {old}, {target} TO {destination}"', strict=True, verbose=True)
        Shell.check(f'{client} --query "DROP TABLE {old} SYNC"', strict=True, verbose=True)
    else:
        Shell.check(f'{client} --query "DROP TABLE {source} SYNC"', strict=True, verbose=True)


POPULATE_DONE_MARKER = "test._populate_done"


def populate_data(port):
    # Rebuild the hits datasets on one server, sequentially. The three inserts
    # share the per-user memory limit (~28GiB) and hits_100m_single alone uses
    # ~21GiB, so running them in parallel is killed by the OvercommitTracker.
    # A dedicated marker table is created only after all three tables are
    # rebuilt: it is the "done" signal for the re-entrant restart() skip. Table
    # existence cannot serve as the marker, because the in-place *_single tables
    # already exist (attached from the tarball) before they are rebuilt.
    client = f"clickhouse-client --port {port}"
    if Shell.get_output(f'{client} --query "EXISTS TABLE {POPULATE_DONE_MARKER}"').strip() == "1":
        print(f"populate_data: server {port} already populated, skipping")
        return
    Shell.check(f'{client} --query "CREATE DATABASE IF NOT EXISTS test"', strict=True, verbose=True)
    # Scope: only the hits datasets are rebuilt (they back the bulk of the
    # suite, including clickbench). The other attached datasets (tpch, tpcds,
    # values) still read their frozen tarball parts, so write-time defaults are
    # not yet exercised on those workloads.
    rebuild_table(port, "default.hits_10m_single", "default.hits_10m_single")
    rebuild_table(port, "default.hits_100m_single", "default.hits_100m_single")
    rebuild_table(port, "datasets.hits_v1", "test.hits")
    Shell.check(f'{client} --query "CREATE TABLE {POPULATE_DONE_MARKER} (done UInt8) ENGINE = Log"', strict=True, verbose=True)


def populate_data_both(left_port, right_port):
    # Populate both servers in parallel. Each writes its own parts, so a PR that
    # changes a write-time default is reflected only on the right (patched) side.
    errors = []

    def run(port):
        try:
            populate_data(port)
        except Exception as e:  # noqa: BLE001
            print(f"populate_data failed on port {port}: {e}")
            errors.append(e)

    threads = [Thread(target=run, args=(p,)) for p in (left_port, right_port)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return not errors


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

    # release_version = CHVersion.get_release_version()
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
        Utils.raise_with_error("Unknown processor architecture")

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
        # The test definitions must stay at the reference vintage (an old server cannot run new
        # queries), but their runner `perf.py` is driven by this job and must match its version.
        Shell.check(
            f"rm -rf ./tests/performance && git checkout {reference_sha} ./tests/performance"
            " && git checkout HEAD -- ./tests/performance/scripts/perf.py",
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

    # Fix the check start time once, for the whole job: the system log export,
    # `compare.sh` and the report uploads must all stamp the same run identity,
    # otherwise the exported `system.*_log` rows cannot be correlated with the
    # perf report of the same shard on `check_start_time`.
    os.environ.setdefault("CHPC_CHECK_START_TIMESTAMP", str(int(Utils.timestamp())))

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
            # The reference (left) binary is the master build, which predates settings this PR adds to
            # keeper_port.xml and rejects them as UNKNOWN_SETTING, so it fails to start. Strip such
            # settings; both sides must share an identical config anyway, and their values are
            # irrelevant to query performance.
            f"sed -i '/<log_readahead_commit_window_bytes>/d' {perf_right_config}/config.d/keeper_port.xml",
            f"chmod +x {ch_path}/clickhouse",
            # The reference build (left) is downloaded as a bare `clickhouse`
            # binary, but the patched build (right) was only symlinked under its
            # subcommand names below. Shell-script perf queries
            # (<query type="shell">) invoke the multi-call binary directly via
            # $CLICKHOUSE_BINARY / $CLICKHOUSE_LOCAL / $CLICKHOUSE_CLIENT, which
            # compare.sh builds from `right/clickhouse`; without this symlink
            # `right/clickhouse local` fails with "No such file or directory" and
            # the query is dropped from the comparison. Mirror the reference
            # layout so `right/clickhouse` exists too.
            f"ln -sf {ch_path}/clickhouse {perf_right}/clickhouse",
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
            stop_watch = Utils.Stopwatch()
            errors = download_and_extract_datasets(dataset_paths.values(), db_path)
            res = not errors
            results.append(
                Result(
                    name="Download datasets",
                    status=Result.Status.OK if res else Result.Status.ERROR,
                    start_time=stop_watch.start_time,
                    duration=stop_watch.duration,
                    info="\n".join(errors),
                )
            )
            if res:
                Shell.check(f"touch {db_path}/.done")

    if res and JobStages.CONFIGURE in stages:
        print("Configure")

        commands = [
            f'echo "ATTACH DATABASE default ENGINE=Ordinary" > {db_path}/metadata/default.sql',
            f'echo "ATTACH DATABASE datasets ENGINE=Ordinary" > {db_path}/metadata/datasets.sql',
            f"ls {db_path}/metadata",
            # Not to disable `text_log` - it is enabled (see
            # tests/performance/scripts/config/config.d/zzz-perf-comparison-tweaks-config.xml)
            # and exported - only to keep its default flush interval instead of
            # the shorter one this file sets.
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
            # Make copies of the original db for both servers. Use hardlinks instead
            # of copying to save space. The datasets are attached as-is; each
            # server re-inserts them into its final tables on startup (see
            # populate_data), so the parts are written by that server's own
            # binary and settings instead of the frozen tarball format.
            f"rm -rf {perf_left}/db {perf_right}/db",
            f"rm -rf {db_path}/preprocessed_configs {db_path}/data/system {db_path}/metadata/system {db_path}/status",
            f"cp -al {db_path} {perf_left}/db ||:",
            f"cp -al {db_path} {perf_right}/db ||:",
            # Each server bootstraps its own (embedded, non-replicated) keeper, so
            # an empty storage dir is enough.
            f"mkdir -p {perf_left}/coordination {perf_right}/coordination",
            # Symlink user_files from the repository into both servers' user_files directories
            f'for f in ./tests/performance/user_files/*; do [ -e "$f" ] || continue; ln -sf "$(readlink -f "$f")" {perf_left}/db/user_files/; ln -sf "$(readlink -f "$f")" {perf_right}/db/user_files/; done',
            # On x86_64, cap max_threads at the number of pinned physical
            # cores (must run after the right->left config copy above).
            write_max_threads_override,
            # Same: the CI Logs cluster must be in the config of both servers.
            create_log_export_configs,
        ]
        results.append(Result.from_commands_run(name="Configure", command=commands))
        res = results[-1].is_ok()

    leftCH = CHServer(is_left=True)
    rightCH = CHServer(is_left=False)
    log_export_servers = (("left", leftCH), ("right", rightCH))

    if res and JobStages.RESTART in stages:
        print("Start Servers")

        match_reference_debug_info()

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

    if res and JobStages.RESTART in stages and not info.is_local_run:
        # After both servers are up and before anything is measured: from here
        # on their system log records are picked up by the export views, and
        # kept locally until the Export system logs stage below.
        results.append(
            Result.from_commands_run(
                name="Start system log export",
                command=start_log_export,
                command_args=[log_export_servers],
                with_info=True,
            )
        )

    if res and JobStages.RESTART in stages:
        print("Populate datasets")

        def populate():
            return populate_data_both(
                CHServer.LEFT_SERVER_PORT, CHServer.RIGHT_SERVER_PORT
            )

        results.append(Result.from_commands_run(name="Populate", command=[populate]))
        res = results[-1].is_ok()

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

        def cleanup_user_files():
            # Tests can write into user_files (INSERT INTO FUNCTION file(...)) and nothing else removes those files.
            # drop_query only drops tables. Keep the symlinks made in Configure, remove everything else.
            for server_path in (perf_left, perf_right):
                user_files = Path(server_path) / "db" / "user_files"
                if not user_files.is_dir():
                    continue
                for entry in user_files.iterdir():
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        shutil.rmtree(entry)
                    else:
                        entry.unlink()

        def run_tests():
            # Run 10 random queries per test by default, but all queries for benchmarks
            benchmarks = {"clickbench.xml", "tpch.xml", "tpcds.xml"}
            for test in test_files:
                max_queries = 0 if test in benchmarks else 10
                CHServer.run_test(
                    "./tests/performance/" + test,
                    max_queries=max_queries,
                    pr_number=info.pr_number,
                    results_path=perf_wd,
                )
                cleanup_user_files()
            return True

        commands = [
            run_tests,
        ]
        results.append(Result.from_commands_run(name="Tests", command=commands))
        res = results[-1].is_ok()

    if JobStages.EXPORT_LOGS in stages and not info.is_local_run:
        # Release the system log records the two servers accumulated while the
        # tests were running. Strictly after the Tests stage, so that sending
        # them does not affect the measurements, and before the Report stage,
        # which stops the servers (compare.sh::get_profiles). Runs even if the
        # tests failed - the logs are the most valuable then. Best effort: it
        # must never fail the job.
        results.append(
            Result.from_commands_run(
                name="Export system logs",
                command=export_system_logs,
                command_args=[log_export_servers],
                with_info=True,
            )
        )

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
        # `CHPC_CHECK_START_TIMESTAMP` is initialized once at the start of the
        # job - do not reset it here, the export stage has already used it.

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

        # insert test cases result generated by legacy script as tsv file into praktika Result object - so that they are written into DB later
        import_ci_checks_results(f"{perf_wd}/ci-checks.tsv", results)

        res = results[-1].is_ok()

    if res and not info.is_local_run and JobStages.REPORT in stages:

        def insert_raw_query_metrics_data():
            cidb = CIDBCluster()
            # Metrics insertion is a reporting side-effect, not the perf
            # verdict. A transient LogCluster (play.clickhouse.com) timeout
            # must not fail the whole job - skip and warn, like
            # insert_report_aggregates() and prepare_historical_data() do.
            if not cidb.is_ready():
                print("WARNING: CIDB not ready - skipping raw query metrics insert")
                return True

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
            # Reporting side-effect, not the perf verdict - a transient
            # LogCluster timeout must not fail the job (see
            # insert_raw_query_metrics_data / insert_report_aggregates).
            if not cidb.is_ready():
                print("WARNING: CIDB not ready - skipping historical data insert")
                return True

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
            if "errors" in message.lower():
                status = Result.Status.FAIL
            elif compare_against_release and message:
                # The release-base comparison is cumulative, so gate on the
                # delta against the previous master run instead of the
                # absolute count (see SLOWER_QUERIES_DELTA_FAIL_THRESHOLD).
                cur_slower = parse_slower_count(message.lower())
                release_base_sha = (
                    info.get_kv_data("release_branch_base_sha_with_predecessors")
                    or [""]
                )[0]
                prev_slower, prev_sha = (None, None)
                if release_base_sha:
                    # Record the baseline this count was measured against, so
                    # that the next master run can tell whether its own count
                    # is comparable (a release cut moves the baseline and
                    # resets the counts).
                    message += format_release_base_marker(release_base_sha)
                    prev_slower, prev_sha = find_prev_master_slower_count(
                        info.job_name,
                        info.get_kv_data("master_track_commits_sha") or [],
                        release_base_sha,
                    )
                if prev_slower is None:
                    print(
                        "WARNING: no usable previous master run found, "
                        "falling back to the absolute slower-count gate"
                    )
                    if too_many_slow(message.lower()):
                        status = Result.Status.FAIL
                else:
                    delta = cur_slower - prev_slower
                    message += (
                        f"; delta vs prev master run ({prev_sha[:8]}): {delta:+d}"
                    )
                    if delta > SLOWER_QUERIES_DELTA_FAIL_THRESHOLD:
                        status = Result.Status.FAIL
            elif too_many_slow(message.lower()):
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
            check_sub_results = build_check_results_children(
                tests_result, f"%Performance%{arch}%master_head%"
            )

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
        f'cd {perf_wd} && find . -type f \( -name "*.log" -o -name "*.tsv" -o -name "*.txt" -o -name "*.rep" -o -name "*.svg" \) ! -path "*/db/*" !  -path "*/db0/*" ! -name "*-trace-log.tsv" -print0 | tar --null -T - -cf - | zstd -o ./logs.tar.zst',
        verbose=True,
    )
    if Path(f"{perf_wd}/logs.tar.zst").is_file():
        files_to_attach.append(f"{perf_wd}/logs.tar.zst")

    result = Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=files_to_attach + [f"{perf_wd}/report/all-query-metrics.tsv"],
        info=message,
    )
    if info.pr_number:
        dashboard_link = (
            f"https://performance.ci.clickhouse.com/runs?q={info.pr_number}"
        )
    else:
        dashboard_link = (
            f"https://performance.ci.clickhouse.com/runs?scope=master&q={(info.sha or '')[:12]}"
        )
    result.set_label(
        "Performance dashboard",
        link=dashboard_link,
        hint="Combined performance dashboard for this run (all shards, amd + arm)",
    )
    result.complete_job()


if __name__ == "__main__":
    main()
