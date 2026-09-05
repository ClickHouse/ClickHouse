"""The HTTP twin of 05055_promql_over_distributed.sql: raw samples are read on every shard and
PromQL runs on the initiator, so every answer must equal a single local table's."""

import json

import pytest

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import (
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    keyed_result,
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

# The Distributed wrapper over the two shard-local TimeSeries tables, and the single local
# TimeSeries table holding the union of the same data.
DIST = "/dist/api/v1"
LOCAL = "/local/api/v1"

EVALUATION_TIME = 140

# The same five series as 05055, sharded on the `host` tag: h1,h2 hash to one shard and h3..h5 to
# the other, so both jobs of `m` straddle the shards and no single shard can answer an aggregation.
INSERT_TEST_DATA = """
INSERT INTO ts_dist (metric_name, tags, time_series) VALUES
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
        # The oracle holds exactly what the shards hold, read back through the wrapper.
        node.query(
            "INSERT INTO ts_all (metric_name, tags, time_series) "
            "SELECT metric_name, tags, time_series FROM ts_dist"
        )
        yield cluster
    finally:
        cluster.shutdown()


def query(handler, promql):
    return keyed_result(
        json.loads(
            execute_query_via_http_api(
                node.ip_address, 9093, f"{handler}/query", promql, EVALUATION_TIME
            )
        )
    )


def range_query(handler, promql, start, end, step):
    return keyed_result(
        json.loads(
            execute_range_query_via_http_api(
                node.ip_address,
                9093,
                f"{handler}/query_range",
                promql,
                start,
                end,
                step,
            )
        )
    )


def values_of(result):
    return {labels: float(value[1]) for labels, value in result[1].items()}


def test_sharding_key_splits_the_metric_across_both_shards():
    # Without this every aggregation below would be answerable by a single shard on its own,
    # and none of the tests would say anything about the fan-out.
    assert (
        node.query(
            "SELECT tags['job'] AS job, uniqExact(_shard_num) AS shards FROM ts_dist "
            "WHERE metric_name = 'm' GROUP BY job ORDER BY job"
        )
        == "a\t2\nb\t2\n"
    )


def test_instant_selector_matches_the_local_table():
    distributed = query(DIST, "m")
    assert distributed == query(LOCAL, "m")
    # Not vacuous: all four series of `m` are there, with their samples at t=140.
    assert distributed[0] == "vector"
    assert sorted(values_of(distributed).values()) == [5.0, 50.0, 500.0, 5000.0]


def test_rate_matches_the_local_table():
    distributed = query(DIST, "rate(m[40s])")
    assert distributed == query(LOCAL, "rate(m[40s])")
    # Every sample of a series has to reach the same group, whichever shard it came from.
    assert distributed[0] == "vector"
    assert len(distributed[1]) == 4
    assert all(value > 0 for value in values_of(distributed).values())


def test_sum_by_job_matches_the_local_table():
    distributed = query(DIST, "sum by (job) (m)")
    assert distributed == query(LOCAL, "sum by (job) (m)")
    # One row per job, each totalling a series taken from each of the two shards.
    assert distributed[0] == "vector"
    assert values_of(distributed) == {
        (("job", "a"),): 55.0,
        (("job", "b"),): 5500.0,
    }


def test_range_query_matches_the_local_table():
    distributed = range_query(DIST, "sum by (job) (m)", 120, EVALUATION_TIME, "10")
    assert distributed == range_query(
        LOCAL, "sum by (job) (m)", 120, EVALUATION_TIME, "10"
    )
    assert distributed[0] == "matrix"
    assert {
        labels: [float(value) for _, value in samples]
        for labels, samples in distributed[1].items()
    } == {
        (("job", "a"),): [33.0, 44.0, 55.0],
        (("job", "b"),): [3300.0, 4400.0, 5500.0],
    }


def test_metric_present_on_one_shard_only_matches_the_local_table():
    distributed = query(DIST, "solo")
    assert distributed == query(LOCAL, "solo")
    assert values_of(distributed) == {(("__name__", "solo"), ("host", "h5")): 11.0}
