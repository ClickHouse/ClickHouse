"""A row policy on a Distributed target does not change the PromQL answer.

PromQL reads a TimeSeries table through its selector, which does not apply a row policy attached
to the table - that is how a single-node TimeSeries table already behaves. The distributed path
inherits it, so both answers are the unfiltered ones and stay equal to each other.

This is a deliberate contract, not an accident: the tests below pin both halves of it, that the
policy really is in force (it empties an ordinary SELECT) and that the PromQL answer over the
wrapper is nevertheless the same as over the equivalent local TimeSeries table.
"""

import contextlib
import json

import pytest

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import execute_query_via_http_api

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

EVALUATION_TIME = 140

# The same series, tags and timestamps as 05055: `h1` and `h2` hash to one shard and `h3`, `h4`,
# `h5` to the other, so both jobs of `m` straddle the two shards.
INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, time_series) VALUES
    ('m', map('job', 'a', 'host', 'h1'),
        [(toDateTime64(100, 3), 1), (toDateTime64(120, 3), 3), (toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'),
        [(toDateTime64(100, 3), 10), (toDateTime64(120, 3), 30), (toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'),
        [(toDateTime64(100, 3), 100), (toDateTime64(120, 3), 300), (toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'),
        [(toDateTime64(100, 3), 1000), (toDateTime64(120, 3), 3000), (toDateTime64(140, 3), 5000)])
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
        # The oracle holds exactly what the shards hold, read back through the wrapper.
        node.query(
            "INSERT INTO ts_all (metric_name, tags, time_series) "
            "SELECT metric_name, tags, time_series FROM ts_dist"
        )
        yield cluster
    finally:
        cluster.shutdown()


@contextlib.contextmanager
def restrictive_row_policies():
    """A policy on each table that matches no row of either."""
    node.query(
        "CREATE ROW POLICY p_ts_dist ON ts_dist USING metric_name = 'nothing_matches' TO ALL"
    )
    node.query(
        "CREATE ROW POLICY p_ts_all ON ts_all USING metric_name = 'nothing_matches' TO ALL"
    )
    try:
        yield
    finally:
        node.query("DROP ROW POLICY p_ts_dist ON ts_dist")
        node.query("DROP ROW POLICY p_ts_all ON ts_all")


def keyed_result(data_json):
    """Keys the series of a query result by their labels: an instant result comes back in no
    defined order, so it has to be keyed before two answers can be compared."""
    data = json.loads(data_json)
    keyed = {
        tuple(sorted(series["metric"].items())): series["value"]
        for series in data["result"]
    }
    assert len(keyed) == len(data["result"]), f"Duplicate label sets in {data_json}"
    return data["resultType"], keyed


def query(handler, promql):
    return keyed_result(
        execute_query_via_http_api(
            node.ip_address, 9093, f"{handler}/query", promql, EVALUATION_TIME
        )
    )


def test_the_row_policies_are_in_force():
    # Without this the test below would pass even if `CREATE ROW POLICY` had done nothing at all.
    assert node.query("SELECT count() FROM ts_dist").strip() != "0"
    assert node.query("SELECT count() FROM ts_all").strip() != "0"
    with restrictive_row_policies():
        assert node.query("SELECT count() FROM ts_dist").strip() == "0"
        assert node.query("SELECT count() FROM ts_all").strip() == "0"


@pytest.mark.parametrize(
    "promql, expected_series", [("m", 4), ("sum by (job) (m)", 2)]
)
def test_row_policy_leaves_the_promql_answer_unchanged(promql, expected_series):
    unfiltered_dist = query(DIST, promql)
    unfiltered_local = query(LOCAL, promql)
    assert unfiltered_dist == unfiltered_local
    assert len(unfiltered_dist[1]) == expected_series

    with restrictive_row_policies():
        # The policy empties an ordinary SELECT through either table (see the test above), and
        # reaches neither PromQL answer - on the distributed path exactly as on the local one.
        assert query(DIST, promql) == unfiltered_dist
        assert query(LOCAL, promql) == unfiltered_local
