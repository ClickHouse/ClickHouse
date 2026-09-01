"""A shard skipped as unavailable leaves no verdict behind.

The shard-target check keeps a passing verdict for a minute. Under `skip_unavailable_shards = 1` a
request served while one shard is down passes too, but it has seen nothing of that shard, which
may come back as anything. Only a verdict that saw every shard is kept, so the first write after
the shard returns is checked again.
"""

import json

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/prometheus_dist.xml",
        "configs/config.d/two_nodes_dist.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/two_nodes_dist.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

START_TIME = 1724112000

# cityHash64('h1') is odd, so every sample written below is routed to node2.
WRITTEN_HOST = "h1"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node1.query("CREATE TABLE ts_local ENGINE = TimeSeries")
        node2.query("CREATE TABLE ts_local ENGINE = TimeSeries")
        # The shape the second shard comes back in: the same name and outer schema, wrong engine.
        node2.query(
            "CREATE TABLE mt_local AS ts_local ENGINE = MergeTree ORDER BY tuple()"
        )
        node1.query(
            "CREATE TABLE prom_dist AS ts_local "
            "ENGINE = Distributed(two_nodes_dist, default, ts_local, cityHash64(tags['host'])) "
            "SETTINGS skip_unavailable_shards = 1"
        )
        node1.query(
            "INSERT INTO ts_local (metric_name, tags, time_series) "
            f"VALUES ('m', map('host', 'h0'), [(toDateTime64({START_TIME}, 3), 1)])"
        )
        yield cluster
    finally:
        cluster.shutdown()


def write(metric_name):
    time_series = [({"__name__": metric_name, "host": WRITTEN_HOST}, {START_TIME: 1.0})]
    return get_response_to_remote_write(
        node1.ip_address,
        9093,
        "/dist/write",
        convert_time_series_to_protobuf(time_series),
    )


def series_count(metric_name):
    return f"SELECT count() FROM timeSeriesTags(ts_local) WHERE metric_name = '{metric_name}'"


def test_a_verdict_that_skipped_a_shard_is_not_kept():
    with PartitionManager() as pm:
        pm.partition_instances(
            node1, node2, port=9000, action="REJECT --reject-with tcp-reset"
        )
        # Served by node1 alone, as skip_unavailable_shards = 1 promises: the check behind it
        # passed without seeing node2.
        answer = json.loads(
            execute_query_via_http_api(
                node1.ip_address, 9093, "/api/v1/query", "m", START_TIME
            )
        )
        assert len(answer["result"]) == 1, answer
        node2.query("EXCHANGE TABLES ts_local AND mt_local")

    # node2 is reachable again, and its `ts_local` is a MergeTree table now.
    response = write("after_outage")
    assert response.status_code >= 400, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text

    # Back as a TimeSeries table, the very next write is accepted and lands on node2.
    node2.query("EXCHANGE TABLES ts_local AND mt_local")
    assert write("after_restore").status_code == 204
    assert_eq_with_retry(node2, series_count("after_restore"), "1")
    # The refused write reached nothing: not the MergeTree table, not a TimeSeries table.
    assert node2.query("SELECT count() FROM mt_local").strip() == "0"
    assert node1.query(series_count("after_outage")).strip() == "0"
    assert node2.query(series_count("after_outage")).strip() == "0"
