"""
The SQL generated for a PromQL query marks subqueries referenced by more than one plan step
`AS MATERIALIZED` so they are evaluated once. That mark is honored by the analyzer only, so
the Prometheus HTTP API must run the generated SQL with the analyzer regardless of the
`enable_analyzer` value the request or the user profile carries.

The assertions read `read_rows` from `system.query_log`: the two interpreters return the same
samples, only the amount of work differs, so a result comparison would pass either way.
"""

import urllib.parse
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    extract_data_from_http_api_response,
    get_response_to_http_api,
)


cluster = ClickHouseCluster(__file__)

# `use_old_analyzer=True` puts `allow_experimental_analyzer = 0` in the default profile, which is
# the way the old-analyzer CI runs reach this code and needs no per-request parameter.
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus.xml",
        "configs/config.d/query_log.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
    use_old_analyzer=True,
)

# Two metrics of 16 series with 8 samples each. Small, and still enough that the duplicated
# scan of the shared subquery is an unambiguous difference in `read_rows`.
SERIES_COUNT = 16
POINTS_PER_SERIES = 8
FIRST_TIMESTAMP = 100
TIMESTAMP_STEP = 10
LAST_TIMESTAMP = FIRST_TIMESTAMP + (POINTS_PER_SERIES - 1) * TIMESTAMP_STEP

# `or` and `topk` both reference their operand subquery from two plan steps, so both get the
# `AS MATERIALIZED` mark. `sum` references it once, so it is never materialized and its
# `read_rows` must not depend on the interpreter - that is what makes it the control.
SHARED_SUBQUERY_QUERIES = [
    "last_over_time(shared_a[100]) or last_over_time(shared_b[100])",
    "topk(3, last_over_time(shared_a[100]))",
]
NO_SHARED_SUBQUERY_QUERY = "sum(last_over_time(shared_a[100]))"

# `read_rows` ceiling per shared shape, as a multiple of what the single-scan control reads.
# Each value sits between the shape's materialized and unmaterialized measurements: `or`
# reads 624 materialized and 880 not, `topk` 320 and 576, against a 288 single scan.
UNMATERIALIZED_READ_ROWS_BOUND = {
    SHARED_SUBQUERY_QUERIES[0]: 2.5,
    SHARED_SUBQUERY_QUERIES[1]: 1.5,
}


def samples_table_name():
    """The inner samples table of the `prometheus` TimeSeries table."""
    return node.query(
        "SELECT concat('.inner_id.samples.', toString(uuid)) FROM system.tables "
        "WHERE database = 'default' AND name = 'prometheus'"
    ).strip()


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(
            "INSERT INTO prometheus (metric_name, tags, time_series) "
            "SELECT metric_name, "
            "       map('instance', concat('i', toString(number))), "
            f"       arrayMap(i -> (toDateTime64({FIRST_TIMESTAMP} + i * {TIMESTAMP_STEP}, 3), "
            "                      toFloat64(number * 10 + i)), "
            f"                range({POINTS_PER_SERIES})) "
            "FROM (SELECT arrayJoin(['shared_a', 'shared_b']) AS metric_name) AS metrics "
            f"CROSS JOIN (SELECT number FROM numbers({SERIES_COUNT})) AS series"
        )
        # A background merge changes how many rows a scan reads, which would make the
        # `read_rows` comparisons below drift. Merge once, then keep the part count fixed.
        samples_table = samples_table_name()
        node.query(f"OPTIMIZE TABLE `default`.`{samples_table}` FINAL")
        node.query(f"SYSTEM STOP MERGES `default`.`{samples_table}`")
        yield cluster
    finally:
        cluster.shutdown()


def read_rows_for_promql_query(promql, path, params=None):
    """Run a PromQL query over the HTTP API and return its `read_rows` from `system.query_log`."""
    query_id = f"promql-analyzer-{uuid.uuid4()}"

    url = (
        f"http://{node.ip_address}:9093{path}"
        f"?query={urllib.parse.quote_plus(promql, safe='')}"
    )
    if path.endswith("/query_range"):
        url += f"&start={FIRST_TIMESTAMP}&end={LAST_TIMESTAMP}&step={TIMESTAMP_STEP}"
    else:
        url += f"&time={LAST_TIMESTAMP}"
    for name, value in (params or {}).items():
        url += f"&{name}={value}"

    response = get_response_to_http_api(url, headers={"X-ClickHouse-Query-Id": query_id})
    # Raises unless the response is a well-formed success envelope, so a request that failed
    # cannot be mistaken for a cheap plan.
    data = extract_data_from_http_api_response(response)

    node.query("SYSTEM FLUSH LOGS query_log")
    # The response is flushed to the client before the QueryFinish row is queued, so the row
    # need not exist yet when the request returns. Exactly one row is still required.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    row = node.query(
        "SELECT read_rows, Settings['allow_experimental_analyzer'] FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'"
    ).split("\t")
    assert len(row) == 2, f"expected exactly one QueryFinish row, got {row!r}"

    read_rows, analyzer_setting = int(row[0]), row[1].strip()
    # The generated query must record the analyzer as forced on. Compare against '1' rather
    # than against '0': an absent key reads back as an empty string and would pass a `!= '0'`
    # check without the setting ever having been applied.
    assert analyzer_setting == "1", (
        f"generated query ran with allow_experimental_analyzer={analyzer_setting!r}, "
        f"expected '1'"
    )
    assert read_rows > 0, f"query read no rows at all, response was {data!r}"
    return read_rows


@pytest.mark.parametrize("path", ["/api/v1/query", "/api/v1/query_range"])
@pytest.mark.parametrize("promql", SHARED_SUBQUERY_QUERIES)
def test_shared_subquery_is_materialized_with_old_analyzer(path, promql):
    """
    A PromQL query whose subquery is shared by two plan steps must read the same number of rows
    whether or not the request asks for the old analyzer. Before the fix the request with
    `enable_analyzer=0` (and the default profile, which sets it too) fell back to the old
    interpreter, which ignores `AS MATERIALIZED` and rescans the shared subquery.
    """
    forced = read_rows_for_promql_query(promql, path, params={"enable_analyzer": 1})
    profile_default = read_rows_for_promql_query(promql, path)
    explicitly_old = read_rows_for_promql_query(promql, path, params={"enable_analyzer": 0})

    assert profile_default == forced, (
        f"{promql!r} on {path} read {profile_default} rows with the profile default but "
        f"{forced} with enable_analyzer=1; the shared subquery was not materialized"
    )
    assert explicitly_old == forced, (
        f"{promql!r} on {path} read {explicitly_old} rows with enable_analyzer=0 but "
        f"{forced} with enable_analyzer=1; the shared subquery was not materialized"
    )


@pytest.mark.parametrize("path", ["/api/v1/query", "/api/v1/query_range"])
def test_query_without_shared_subquery_is_unaffected(path):
    """
    Control: a PromQL query that references its subquery once is never marked
    `AS MATERIALIZED`, so its `read_rows` is the same in every mode. This fails if the fix
    starts materializing subqueries that should stay inlined.
    """
    forced = read_rows_for_promql_query(
        NO_SHARED_SUBQUERY_QUERY, path, params={"enable_analyzer": 1}
    )
    explicitly_old = read_rows_for_promql_query(
        NO_SHARED_SUBQUERY_QUERY, path, params={"enable_analyzer": 0}
    )
    assert explicitly_old == forced


@pytest.mark.parametrize("path", ["/api/v1/query", "/api/v1/query_range"])
@pytest.mark.parametrize("promql", SHARED_SUBQUERY_QUERIES)
def test_shared_subquery_reads_less_than_unmaterialized(path, promql):
    """
    Guards the assertions above against becoming vacuous. If a shared subquery ever stopped
    being materialized for every mode, the equality checks would still hold while the
    optimization was gone. Reading strictly fewer rows than an unmaterialized plan needs
    cannot be satisfied without materialization. Both shapes are checked because `or` and
    `topk` acquire the mark through independent code paths.
    """
    shared = read_rows_for_promql_query(promql, path)
    single_scan = read_rows_for_promql_query(NO_SHARED_SUBQUERY_QUERY, path)

    bound = UNMATERIALIZED_READ_ROWS_BOUND[promql]
    assert shared < bound * single_scan, (
        f"{promql!r} on {path} read {shared} rows against {single_scan} for a single scan, "
        f"over the {bound}x ceiling that separates a materialized plan from a rescanning one"
    )
