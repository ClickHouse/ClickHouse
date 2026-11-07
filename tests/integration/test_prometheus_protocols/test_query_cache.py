import json

import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    execute_query_via_http_api,
    execute_range_query_via_http_api,
)

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
            "CREATE TABLE prometheus (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries"
        )
        node.query(
            "INSERT INTO prometheus (metric_name, tags, samples)"
            " VALUES ('up', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 1)])"
        )
        for i in range(2, 5):
            node.query(
                "INSERT INTO prometheus (metric_name, tags, samples)"
                f" VALUES ('foo', {{'instance':'host{i}'}}, [(toDateTime64(1700000000, 3, 'UTC'), {i})])"
            )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clear_query_cache():
    node.query("SYSTEM DROP QUERY CACHE")
    yield


def run_instant_query(params, expect_error=False, timestamp=EVALUATION_TIME, query="up"):
    result = execute_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query",
        query,
        timestamp=timestamp,
        params=params,
        expect_error=expect_error,
    )
    return result if expect_error else json.loads(result)


def run_range_query(params):
    result = execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query_range",
        "up",
        EVALUATION_TIME - 60,
        EVALUATION_TIME,
        30,
        params=params,
    )
    return json.loads(result)


def get_query_cache_hits():
    return int(node.query("SELECT sum(value) FROM system.events WHERE event = 'QueryCacheHits'"))


def test_query_cache_rejects_current_time_by_default():
    # Without `time` the query is evaluated at the current time, so the default
    # query_cache_nondeterministic_function_handling = 'throw' must reject it.
    error = run_instant_query({"use_query_cache": 1}, expect_error=True, timestamp=None)
    assert "non-deterministic" in error
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


def test_query_cache_respects_explicit_handling():
    params = {
        "use_query_cache": 1,
        "query_cache_nondeterministic_function_handling": "ignore",
    }
    assert run_instant_query(params)["result"][0]["value"] == [EVALUATION_TIME, "1"]
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0


@pytest.mark.parametrize("handling", ["ignore", "throw"])
def test_query_cache_explicit_handling_skips_cached_result(handling):
    run_instant_query({"use_query_cache": 1})
    assert int(node.query("SELECT count() FROM system.query_cache")) == 1

    params = {
        "use_query_cache": 1,
        "query_cache_nondeterministic_function_handling": handling,
    }
    hits_before = get_query_cache_hits()
    if handling == "ignore":
        assert run_instant_query(params)["result"][0]["value"] == [EVALUATION_TIME, "1"]
    else:
        assert "non-deterministic" in run_instant_query(params, expect_error=True)
    assert get_query_cache_hits() == hits_before


def test_query_cache_stores_and_hits():
    params = {"use_query_cache": 1}
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


def test_query_cache_stores_and_hits_range_query():
    params = {"use_query_cache": 1}
    expected = {
        "resultType": "matrix",
        "result": [
            {
                "metric": {"__name__": "up", "instance": "host1"},
                "values": [[EVALUATION_TIME, "1"]],
            }
        ],
    }

    assert run_range_query(params) == expected
    assert int(node.query("SELECT count() FROM system.query_cache")) == 1

    hits_before = get_query_cache_hits()
    assert run_range_query(params) == expected
    assert get_query_cache_hits() == hits_before + 1


@pytest.mark.parametrize("handling", [None, "save"])
def test_query_cache_does_not_cache_subqueries(handling):
    # A cached generated subquery loses its per-query state and drops the `up` series.
    params = {"use_query_cache": 1, "query_cache_for_subqueries": 1}
    if handling is not None:
        params["query_cache_nondeterministic_function_handling"] = handling
    run_instant_query(params)
    assert len(run_instant_query(params, query="foo or up")["result"]) == 4
    assert int(node.query("SELECT count() FROM system.query_cache WHERE is_subquery")) == 0
