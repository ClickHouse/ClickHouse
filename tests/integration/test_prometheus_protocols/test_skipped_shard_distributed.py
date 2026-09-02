"""The shard-target check asks every replica, and never vouches for one it could not reach.

A passing verdict is kept for a minute. A read served while one replica is down passes too, but
it has seen nothing of that replica, which may come back as anything: only a verdict that saw
every replica is kept, so the first request after the replica returns is checked again.

A write is refused outright while a replica is unreachable. The sink would queue the samples for
it and the background sender would deliver them once it is back, without any check of its own.
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
        # Its own shard tables, so the tests below never share a cached verdict.
        node1.query("CREATE TABLE ts_queue ENGINE = TimeSeries")
        node2.query("CREATE TABLE ts_queue ENGINE = TimeSeries")
        node2.query(
            "CREATE TABLE mt_queue AS ts_queue ENGINE = MergeTree ORDER BY tuple()"
        )
        node1.query(
            "CREATE TABLE prom_queue AS ts_local "
            "ENGINE = Distributed(two_nodes_dist, default, ts_queue, cityHash64(tags['host']))"
        )
        # One shard whose two replicas are node1 and node2; no internal_replication, so the sink
        # writes every replica itself.
        node1.query("CREATE TABLE ts_rep ENGINE = TimeSeries")
        node2.query("CREATE TABLE ts_rep ENGINE = TimeSeries")
        node2.query("CREATE TABLE mt_rep AS ts_rep ENGINE = MergeTree ORDER BY tuple()")
        node1.query(
            "CREATE TABLE prom_rep AS ts_local "
            "ENGINE = Distributed(one_shard_two_replicas, default, ts_rep)"
        )
        node1.query(
            "INSERT INTO ts_local (metric_name, tags, time_series) "
            f"VALUES ('m', map('host', 'h0'), [(toDateTime64({START_TIME}, 3), 1)])"
        )
        yield cluster
    finally:
        cluster.shutdown()


def write(metric_name, path="/dist/write"):
    time_series = [({"__name__": metric_name, "host": WRITTEN_HOST}, {START_TIME: 1.0})]
    return get_response_to_remote_write(
        node1.ip_address,
        9093,
        path,
        convert_time_series_to_protobuf(time_series),
    )


def series_count(metric_name, table="ts_local"):
    return f"SELECT count() FROM timeSeriesTags({table}) WHERE metric_name = '{metric_name}'"


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


def test_a_write_over_an_unreachable_shard_is_refused_not_queued():
    with PartitionManager() as pm:
        pm.partition_instances(
            node1, node2, port=9000, action="REJECT --reject-with tcp-reset"
        )
        # `prom_queue` declares no skip setting, so the sink would queue the samples for node2:
        # the write is refused instead, with an error Prometheus retries.
        response = write("during_outage", "/queue/write")
        assert response.status_code >= 500, response.text
        assert "ALL_CONNECTION_TRIES_FAILED" in response.text
        pending = node1.query(
            "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'prom_queue'"
        )
        assert pending.strip() == "0", pending
        # What a queued file would have been delivered to once node2 answered again.
        node2.query("EXCHANGE TABLES ts_queue AND mt_queue")

    node1.query("SYSTEM FLUSH DISTRIBUTED prom_queue")
    assert node2.query("SELECT count() FROM ts_queue").strip() == "0"
    assert node2.query(series_count("during_outage", "mt_queue")).strip() == "0"
    assert node1.query(series_count("during_outage", "ts_queue")).strip() == "0"

    # Refused while the target is wrong, then accepted and delivered once it is restored.
    response = write("after_swap", "/queue/write")
    assert response.status_code >= 400, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    node2.query("EXCHANGE TABLES ts_queue AND mt_queue")
    assert write("after_restore", "/queue/write").status_code == 204
    node1.query("SYSTEM FLUSH DISTRIBUTED prom_queue")
    assert node2.query(series_count("after_restore", "ts_queue")).strip() == "1"
    assert node2.query("SELECT count() FROM mt_queue").strip() == "0"


def test_every_replica_is_checked_not_one_per_shard():
    # node2's replica of the shard is a MergeTree table, node1's a TimeSeries table: a cluster()
    # probe would have asked only one of them.
    node2.query("EXCHANGE TABLES ts_rep AND mt_rep")
    response = write("two_replicas", "/rep/write")
    assert response.status_code >= 400, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    error = node1.query_and_get_error(
        f"SELECT * FROM prometheusQuery(prom_rep, 'm', {START_TIME})"
    )
    assert "UNEXPECTED_TABLE_ENGINE" in error
    # The refused write reached neither replica.
    assert node2.query("SELECT count() FROM ts_rep").strip() == "0"
    assert node1.query(series_count("two_replicas", "ts_rep")).strip() == "0"

    # With both replicas TimeSeries tables, the write is accepted and the sink writes both.
    node2.query("EXCHANGE TABLES ts_rep AND mt_rep")
    assert write("two_replicas", "/rep/write").status_code == 204
    assert_eq_with_retry(node1, series_count("two_replicas", "ts_rep"), "1")
    assert_eq_with_retry(node2, series_count("two_replicas", "ts_rep"), "1")
    assert node2.query("SELECT count() FROM mt_rep").strip() == "0"
