import json

import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import execute_query_via_http_api

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

EVALUATION_TIME = 1700000000


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE prometheus_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64)"
            " ENGINE = MergeTree ORDER BY (id, timestamp)"
        )
        node.query(
            "CREATE TABLE prometheus_tags (id UUID, metric_name LowCardinality(String),"
            " tags Map(LowCardinality(String), String),"
            " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),"
            " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))"
            " ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"
            " SETTINGS allow_dimensions_outside_sorting_key = 1"
        )
        node.query(
            "CREATE TABLE prometheus_metrics (metric_family_name String, type String, unit String, help String)"
            " ENGINE = ReplacingMergeTree ORDER BY metric_family_name"
        )
        node.query(
            "CREATE TABLE prometheus ENGINE = TimeSeries"
            " DATA prometheus_data TAGS prometheus_tags METRICS prometheus_metrics"
        )
        node.query(
            "INSERT INTO prometheus_tags VALUES ('00000000-0000-0000-0000-000000000001', 'up',"
            " {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'))"
        )
        node.query(
            "INSERT INTO prometheus_data VALUES"
            " ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1)"
        )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clear_query_cache():
    node.query("SYSTEM DROP QUERY CACHE")
    yield


def run_instant_query(params, expect_error=False):
    result = execute_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query",
        "up",
        timestamp=EVALUATION_TIME,
        params=params,
        expect_error=expect_error,
    )
    return result if expect_error else json.loads(result)


def get_query_cache_hits():
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'QueryCacheHits'"))


def test_query_cache_rejected_by_default():
    # The transpiled SQL contains non-deterministic timeSeries* functions, so the default
    # query_cache_nondeterministic_function_handling = 'throw' must reject the query.
    error = run_instant_query({"use_query_cache": 1}, expect_error=True)
    assert "non-deterministic" in error
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


def test_query_cache_stores_and_hits():
    params = {
        "use_query_cache": 1,
        "query_cache_nondeterministic_function_handling": "save",
    }
    expected = {
        "resultType": "vector",
        "result": [
            {
                "metric": {"__name__": "up", "instance": "host1"},
                "value": [EVALUATION_TIME, "1"],
            }
        ],
    }

    assert run_instant_query(params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 1

    hits_before = get_query_cache_hits()
    assert run_instant_query(params) == expected
    assert get_query_cache_hits() == hits_before + 1
