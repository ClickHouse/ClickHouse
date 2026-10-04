import time
import urllib.parse
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# A multiple of the split interval, so the chunk boundaries below are known.
H = 1699999200
INTERVAL = 3600


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE = TimeSeries")
        # Nine hours of samples every 15 seconds; host2/idle resets, host3 has node_load1 only from H + 2h to H + 3h.
        node.query(
            "INSERT INTO prometheus (metric_name, tags, samples)"
            " SELECT 'node_cpu_seconds_total', map('instance', concat('host', toString(h)), 'mode', mode),"
            f" arrayMap(i -> (toDateTime64({H - 3600} + i * 15, 3),"
            " toFloat64(if(h = 2 AND mode = 'idle' AND i >= 1000, i - 1000, i) * (h + indexOf(['idle', 'user', 'system'], mode)) * 1.5)),"
            " range(2160))"
            " FROM (SELECT number + 1 AS h FROM numbers(3)) ARRAY JOIN ['idle', 'user', 'system'] AS mode"
        )
        node.query(
            "INSERT INTO prometheus (metric_name, tags, samples)"
            " SELECT 'node_load1', map('instance', concat('host', toString(h))),"
            f" arrayMap(i -> (toDateTime64({H - 3600} + i * 15, 3), ((i * 7 + h * 13) % 50) / 10),"
            " if(h = 3, range(720, 960), range(2160)))"
            " FROM (SELECT number + 1 AS h FROM numbers(3))"
        )
        yield cluster
    finally:
        cluster.shutdown()


def send_query_range(query, start, end, step, params=None, query_id=None):
    # One thread makes the order of floating-point sums, and so the response bytes, deterministic.
    url_params = {"query": query, "start": start, "end": end, "step": step, "max_threads": 1}
    url_params.update(params or {})
    url = f"http://{node.ip_address}:9093/api/v1/query_range?{urllib.parse.urlencode(url_params)}"
    return requests.get(url, headers={"X-ClickHouse-Query-Id": query_id} if query_id else {})


def query_range(query, start, end, step, params=None, query_id=None):
    response = send_query_range(query, start, end, step, params, query_id)
    assert response.status_code == 200, response.text
    return response.text


def assert_executed_queries(query_id, expected):
    # The unsplit query writes its query_log row after sending the response, so the row can come later.
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        f"{expected}\n",
        retry_count=30,
        sleep_time=1,
    )


def get_query_cache_hits():
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'QueryCacheHits'"))


QUERIES = [
    "sum by (mode) (rate(node_cpu_seconds_total[5m]))",
    "rate(node_cpu_seconds_total[5m])",
    "avg_over_time(node_load1[10m])",
    "max_over_time(rate(node_cpu_seconds_total[5m])[30m:1m])",
    "node_load1 offset 30m",
    "scalar(sum(node_load1))",
]

# (start, end, step, number of chunks)
RANGES = [
    # Aligned to the interval: the last chunk holds only `end`.
    (H, H + 6 * 3600, 60, 7),
    # Not aligned: the chunks start at H + 3634, H + 7234, H + 10834, H + 14434, H + 18034.
    (H + 1234, H + 5 * 3600 + 777, 60, 6),
    # The step doesn't divide the interval: the chunks start at H + 3754, H + 7534, H + 10894, H + 14674, H + 18034.
    (H + 1234, H + 5 * 3600 + 777, 420, 6),
    # The step is longer than the interval: one step per chunk.
    (H, H + 6 * 3600, 5400, 5),
    # Shorter than the interval, so not split.
    (H, H + 3000, 60, 1),
]


@pytest.mark.parametrize("query", QUERIES)
@pytest.mark.parametrize("start, end, step, num_chunks", RANGES)
def test_split_query_matches_unsplit(query, start, end, step, num_chunks):
    expected = query_range(query, start, end, step)
    assert '"result":[]' not in expected

    query_id = f"range-split-{uuid.uuid4()}"
    params = {"promql_range_query_split_interval": INTERVAL}
    assert query_range(query, start, end, step, params, query_id) == expected
    assert_executed_queries(query_id, num_chunks)


def test_start_modifier_is_not_split():
    query = "node_load1 @ start()"
    expected = query_range(query, H, H + 6 * 3600, 60)

    query_id = f"range-split-{uuid.uuid4()}"
    params = {"promql_range_query_split_interval": INTERVAL}
    assert query_range(query, H, H + 6 * 3600, 60, params, query_id) == expected
    assert_executed_queries(query_id, 1)


@pytest.mark.parametrize(
    "start, end, step",
    [
        # The chunk boundaries must not overflow.
        (H, H + 6 * 3600, "106751991167d"),
        # Too many steps for one query.
        (0, 9000000000000, 60),
    ],
)
def test_split_query_fails_like_unsplit(start, end, step):
    expected = send_query_range("node_load1", start, end, step)
    params = {"promql_range_query_split_interval": INTERVAL}
    response = send_query_range("node_load1", start, end, step, params)
    assert response.status_code == expected.status_code == 400
    assert response.text == expected.text


def test_old_chunks_come_from_query_cache():
    node.query("SYSTEM DROP QUERY CACHE")
    query = "sum by (mode) (rate(node_cpu_seconds_total[5m]))"
    start, end, step = H + 1234, H + 5 * 3600 + 777, 60
    expected = query_range(query, start, end, step)

    # Six chunks; the first and the last ones don't cover a whole interval, and the fifth one ends later than now() - min_age.
    min_age = int(time.time()) - (H + 4 * 3600 + 1800)
    params = {
        "promql_range_query_split_interval": INTERVAL,
        "promql_range_query_cache_min_age": min_age,
        "query_cache_ttl": 3600,
    }
    assert query_range(query, start, end, step, params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 3

    # The chunks are found in the query cache although min_age differs.
    params["promql_range_query_cache_min_age"] = min_age + 60
    hits = get_query_cache_hits()
    assert query_range(query, start, end, step, params) == expected
    assert get_query_cache_hits() == hits + 3
    assert int(node.query("SELECT count() FROM system.query_cache")) == 3


def test_negative_offset_is_not_cached():
    node.query("SYSTEM DROP QUERY CACHE")
    query = "node_load1 offset -10m"
    expected = query_range(query, H, H + 6 * 3600, 60)

    params = {
        "promql_range_query_split_interval": INTERVAL,
        "promql_range_query_cache_min_age": 600,
        "query_cache_ttl": 3600,
    }
    assert query_range(query, H, H + 6 * 3600, 60, params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


@pytest.mark.parametrize(
    "query",
    ["sum by (mode) (rate(node_cpu_seconds_total[5m]))", "node_load1 offset -10m", f"node_load1 @ {H + 3600}"],
)
def test_use_query_cache_of_request_caches_no_chunk(query):
    node.query("SYSTEM DROP QUERY CACHE")
    expected = query_range(query, H, H + 6 * 3600, 60)

    # promql_range_query_cache_min_age is 0, so no chunk is cached.
    params = {
        "promql_range_query_split_interval": INTERVAL,
        "use_query_cache": 1,
        "query_cache_nondeterministic_function_handling": "save",
        "query_cache_ttl": 3600,
    }
    assert query_range(query, H, H + 6 * 3600, 60, params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


# Every hour has its own series, so each chunk has one series and the whole range seven.
HOURLY = 'count_values("hour", floor(vector(time() / 3600)))'


@pytest.mark.parametrize(
    "query, params",
    [
        (HOURLY, {"max_result_rows": 3}),
        (HOURLY, {"max_result_rows": 3, "result_overflow_mode": "break", "max_block_size": 2}),
        (HOURLY, {"offset": 1}),
        # The `limit` parameter belongs to the API, so the setting `limit` comes from the user.
        (HOURLY, {"user": "range_split_limit"}),
        ("rate(node_cpu_seconds_total[5m])", {"max_result_bytes": 20000}),
        # host3 is only in the chunk from H + 2h, so merging chunks in this order would repeat series.
        ("node_load1", {"order": "tags DESC"}),
        ("node_load1", {"sort": "-1"}),
        ("sum by (mode) (rate(node_cpu_seconds_total[5m]))", {"select": "tags, arraySlice(samples, 1, 2) AS samples"}),
        ("sum by (mode) (rate(node_cpu_seconds_total[5m]))", {"filter": "length(samples) > 100"}),
    ],
)
def test_whole_result_settings_disable_splitting(query, params):
    node.query("CREATE USER IF NOT EXISTS range_split_limit SETTINGS PROFILE 'default', limit = 3")
    node.query("GRANT SELECT ON *.* TO range_split_limit")
    expected = send_query_range(query, H, H + 6 * 3600, 60, params)

    split_params = {**params, "promql_range_query_split_interval": INTERVAL}
    response = send_query_range(query, H, H + 6 * 3600, 60, split_params)
    assert (response.status_code, response.text) == (expected.status_code, expected.text)


# Every step has its own series, so each chunk has 60 series and the whole range 361.
PER_STEP = 'count_values("v", vector(time()))'


@pytest.mark.parametrize(
    "query, params",
    [
        (PER_STEP, {"max_rows_to_group_by": 100}),
        (PER_STEP, {"max_rows_to_group_by": 30, "group_by_overflow_mode": "any", "max_block_size": 10}),
        (PER_STEP, {"max_rows_to_sort": 100}),
        (PER_STEP, {"max_rows_to_sort": 30, "sort_overflow_mode": "break", "max_block_size": 10}),
        (PER_STEP, {"max_bytes_to_sort": 20000}),
        (f"{PER_STEP} + {PER_STEP}", {"max_rows_in_join": 100}),
        (f"{PER_STEP} + {PER_STEP}", {"max_bytes_in_join": 20000}),
    ],
)
def test_group_by_sort_and_join_limits_disable_splitting(query, params):
    expected = send_query_range(query, H, H + 6 * 3600, 60, params)

    split_params = {**params, "promql_range_query_split_interval": INTERVAL}
    response = send_query_range(query, H, H + 6 * 3600, 60, split_params)
    assert (response.status_code, response.text) == (expected.status_code, expected.text)


# The read limits are high enough for the whole query, but each chunk would start them again, so the query is executed at once.
@pytest.mark.parametrize(
    "params",
    [
        {"max_rows_to_read": 1000000000},
        {"max_bytes_to_read": 1000000000000},
        {"max_rows_to_read_leaf": 1000000000},
        {"max_bytes_to_read_leaf": 1000000000000},
    ],
)
def test_read_limits_disable_splitting(params):
    query = "rate(node_cpu_seconds_total[5m])"
    expected = query_range(query, H, H + 6 * 3600, 60, params)

    query_id = f"range-split-{uuid.uuid4()}"
    split_params = {**params, "promql_range_query_split_interval": INTERVAL}
    assert query_range(query, H, H + 6 * 3600, 60, split_params, query_id) == expected
    assert_executed_queries(query_id, 1)


@pytest.mark.parametrize(
    "overflow_mode",
    [
        "read_overflow_mode",
        "read_overflow_mode_leaf",
        "group_by_overflow_mode",
        "sort_overflow_mode",
        "result_overflow_mode",
        "timeout_overflow_mode",
        "set_overflow_mode",
        "join_overflow_mode",
        "transfer_overflow_mode",
        "distinct_overflow_mode",
    ],
)
def test_chunk_with_non_throw_overflow_mode_is_not_cached(overflow_mode):
    node.query("SYSTEM DROP QUERY CACHE")
    expected = query_range("node_load1", H, H + 6 * 3600, 60, {overflow_mode: "break"})

    params = {
        overflow_mode: "break",
        "promql_range_query_split_interval": INTERVAL,
        "promql_range_query_cache_min_age": 600,
        "query_cache_ttl": 3600,
    }
    assert query_range("node_load1", H, H + 6 * 3600, 60, params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


@pytest.mark.parametrize("params", [{"query_cache_for_subqueries": 1}, {"user": "range_split_subqueries"}])
def test_subqueries_of_cached_chunk_are_not_cached(params):
    node.query("CREATE USER IF NOT EXISTS range_split_subqueries SETTINGS PROFILE 'default', query_cache_for_subqueries = 1")
    node.query("GRANT SELECT, CREATE TEMPORARY TABLE ON *.* TO range_split_subqueries")
    node.query("SYSTEM DROP QUERY CACHE")
    query = "node_cpu_seconds_total or node_load1"
    expected = query_range(query, H, H + 6 * 3600, 60)

    # A subquery of the generated SQL fills or reads the tags of its own query, so it must not come from another query.
    params = {
        **params,
        "promql_range_query_split_interval": INTERVAL,
        "promql_range_query_cache_min_age": 600,
        "query_cache_ttl": 3600,
    }
    query_range("node_load1", H, H + 6 * 3600, 60, params)
    assert query_range(query, H, H + 6 * 3600, 60, params) == expected
    assert node.query("SELECT is_subquery, count() FROM system.query_cache GROUP BY is_subquery") == "0\t12\n"
