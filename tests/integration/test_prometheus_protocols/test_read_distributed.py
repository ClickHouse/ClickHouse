"""PromQL over a Distributed table of per-shard TimeSeries tables, through the Prometheus HTTP API.

The samples are read on every shard and the query is evaluated on the initiator, so every answer
must equal the answer over one local TimeSeries table holding the same data.
"""

import json

import pytest

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import (
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    get_response_to_http_api,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus_distributed.xml",
        "configs/config.d/two_shards_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

DIST = "/dist/api/v1"
LOCAL = "/local/api/v1"

# Sharded on the `host` tag: h1, h2 hash to one shard and h3, h4, h5 to the other, so both jobs
# of `m` straddle the shards and no single shard can answer an aggregation.
INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'host', 'h1'),
        [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 2), (toDateTime64(120, 3), 3),
         (toDateTime64(130, 3), 4), (toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'),
        [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 30),
         (toDateTime64(130, 3), 40), (toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'),
        [(toDateTime64(100, 3), 100), (toDateTime64(110, 3), 200), (toDateTime64(120, 3), 300),
         (toDateTime64(130, 3), 400), (toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'),
        [(toDateTime64(100, 3), 1000), (toDateTime64(110, 3), 2000), (toDateTime64(120, 3), 3000),
         (toDateTime64(130, 3), 4000), (toDateTime64(140, 3), 5000)]),
    ('solo', map('host', 'h5'),
        [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 9),
         (toDateTime64(130, 3), 10), (toDateTime64(140, 3), 11)])
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE shard_0")
        node.query("CREATE DATABASE shard_1")
        node.query("CREATE TABLE shard_0.ts_local ENGINE=TimeSeries")
        node.query("CREATE TABLE shard_1.ts_local ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE ts_dist AS shard_0.ts_local "
            "ENGINE = Distributed(two_shards_dist, '', ts_local, cityHash64(tags['host']))"
        )
        node.query("CREATE TABLE ts_all ENGINE=TimeSeries")
        node.query(INSERT_TEST_DATA, settings={"distributed_foreground_insert": 1})
        node.query(
            "INSERT INTO ts_all (metric_name, tags, samples) "
            "SELECT metric_name, tags, samples FROM ts_dist"
        )
        yield cluster
    finally:
        cluster.shutdown()


def keyed_result(data):
    """An instant result comes back in no defined order, so the series are keyed by their labels."""
    data = json.loads(data)
    keyed = {
        tuple(sorted(series["metric"].items())): series.get("value", series.get("values"))
        for series in data["result"]
    }
    assert len(keyed) == len(data["result"]), f"Duplicate label sets in {data}"
    return data["resultType"], keyed


def test_sharding_key_splits_the_metric_across_both_shards():
    assert node.query(
        "SELECT tags['job'] AS job, uniqExact(_shard_num) FROM ts_dist "
        "WHERE metric_name = 'm' GROUP BY job ORDER BY job"
    ) == "a\t2\nb\t2\n"


@pytest.mark.parametrize(
    "promql, expected_series",
    [
        ("m", 4),
        ("solo", 1),
        ("rate(m[40s])", 4),
        ("sum by (job) (m)", 2),
        ("sum(m)", 1),
        ("topk(2, m)", 2),
        ("sum by (job) (rate(m[40s])) / on () group_left sum(rate(m[40s]))", 2),
        ("m * on (job, host) m", 4),
        ("count(m) + count(solo)", 1),
    ],
)
def test_instant_query_matches_the_local_table(promql, expected_series):
    dist = keyed_result(execute_query_via_http_api(node.ip_address, 9093, f"{DIST}/query", promql, 140))
    local = keyed_result(execute_query_via_http_api(node.ip_address, 9093, f"{LOCAL}/query", promql, 140))
    assert dist == local
    assert len(dist[1]) == expected_series


@pytest.mark.parametrize("promql", ["m", "sum by (job) (m)", "rate(m[30s])"])
def test_range_query_matches_the_local_table(promql):
    dist = keyed_result(
        execute_range_query_via_http_api(node.ip_address, 9093, f"{DIST}/query_range", promql, 100, 140, 10)
    )
    local = keyed_result(
        execute_range_query_via_http_api(node.ip_address, 9093, f"{LOCAL}/query_range", promql, 100, 140, 10)
    )
    assert dist == local
    assert dist[1]


@pytest.mark.parametrize(
    "endpoint",
    ["series?match[]=m", "labels", "label/host/values", "metadata"],
)
def test_other_endpoints_refuse_a_distributed_table(endpoint):
    response = get_response_to_http_api(f"http://{node.ip_address}:9093{DIST}/{endpoint}")
    assert response.status_code != 200
    assert "TimeSeries" in response.json()["error"]


def test_row_policy_refuses_the_query():
    node.query("CREATE ROW POLICY p_dist ON ts_dist USING metric_name = 'solo' TO ALL")
    try:
        error = execute_query_via_http_api(
            node.ip_address, 9093, f"{DIST}/query", "m", 140, expect_error=True
        )
        assert "row policy" in error
    finally:
        node.query("DROP ROW POLICY p_dist ON ts_dist")
    assert keyed_result(
        execute_query_via_http_api(node.ip_address, 9093, f"{DIST}/query", "m", 140)
    )[1]
