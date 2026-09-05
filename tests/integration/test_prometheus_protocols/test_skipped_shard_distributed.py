"""The shard-target check runs afresh on every request, so a table swapped under the name is caught by the
next one; a replica it cannot reach, or without its table, passes a read but refuses a write, never queued.
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
        # Its own shard tables, so the tests below never see each other's swaps.
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
        # Its second shard loses its table for a while.
        node1.query("CREATE TABLE ts_missing ENGINE = TimeSeries")
        node2.query("CREATE TABLE ts_missing ENGINE = TimeSeries")
        node2.query(
            "CREATE TABLE mt_missing AS ts_missing ENGINE = MergeTree ORDER BY tuple()"
        )
        node1.query(
            "CREATE TABLE prom_missing AS ts_local "
            "ENGINE = Distributed(two_nodes_dist, default, ts_missing, cityHash64(tags['host']))"
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


def test_a_shard_table_swapped_after_a_passing_check_is_refused_by_the_next_write():
    # A write the check passed is on node2 before the 204: delivered by the INSERT itself, never
    # queued on node1 to be replayed after the check.
    assert write("before_swap").status_code == 204
    assert node2.query(series_count("before_swap")).strip() == "1"
    assert (
        node1.query(
            "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'prom_dist'"
        ).strip()
        == "0"
    )

    # The same name, the same outer schema, a MergeTree table: the very next write is refused
    # on its own check, not accepted on the strength of the one that just passed.
    node2.query("EXCHANGE TABLES ts_local AND mt_local")
    response = write("after_swap")
    assert response.status_code >= 400, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    error = node1.query_and_get_error(
        f"SELECT * FROM prometheusQuery(prom_dist, 'm', {START_TIME})"
    )
    assert "UNEXPECTED_TABLE_ENGINE" in error

    # Back as a TimeSeries table, the very next write is accepted and lands on node2.
    node2.query("EXCHANGE TABLES ts_local AND mt_local")
    assert write("after_restore").status_code == 204
    assert_eq_with_retry(node2, series_count("after_restore"), "1")
    # The refused write reached nothing: not the MergeTree table, not a TimeSeries table.
    assert node2.query("SELECT count() FROM mt_local").strip() == "0"
    assert node1.query(series_count("after_swap")).strip() == "0"
    assert node2.query(series_count("after_swap")).strip() == "0"


def test_a_declared_skip_covers_a_read_not_a_write():
    with PartitionManager() as pm:
        pm.partition_instances(
            node1, node2, port=9000, action="REJECT --reject-with tcp-reset"
        )
        # Served by node1 alone, as skip_unavailable_shards = 1 promises.
        answer = json.loads(
            execute_query_via_http_api(
                node1.ip_address, 9093, "/api/v1/query", "m", START_TIME
            )
        )
        assert len(answer["result"]) == 1, answer
        # The sink would queue the samples for node2 whatever the table declares: refused instead,
        # with an error Prometheus retries.
        response = write("during_outage")
        assert response.status_code >= 500, response.text
        assert "ALL_CONNECTION_TRIES_FAILED" in response.text
        pending = node1.query(
            "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'prom_dist'"
        )
        assert pending.strip() == "0", pending

    # node2 is reachable again: the very next write is accepted and lands there.
    assert write("after_outage").status_code == 204
    assert_eq_with_retry(node2, series_count("after_outage"), "1")
    assert node1.query(series_count("during_outage")).strip() == "0"
    assert node2.query(series_count("during_outage")).strip() == "0"


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


def test_a_skip_asked_for_by_the_request_does_not_cover_a_write():
    with PartitionManager() as pm:
        pm.partition_instances(
            node1, node2, port=9000, action="REJECT --reject-with tcp-reset"
        )
        # The URL is where `skip_unavailable_shards` reaches the sink, which would drop node2 from
        # the write and answer 204: forced off on this path, so the write is refused like any other.
        response = write("asked_to_skip", "/queue/write?skip_unavailable_shards=1")
        assert response.status_code >= 500, response.text
        assert "ALL_CONNECTION_TRIES_FAILED" in response.text
        pending = node1.query(
            "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'prom_queue'"
        )
        assert pending.strip() == "0", pending

    node1.query("SYSTEM FLUSH DISTRIBUTED prom_queue")
    assert node1.query(series_count("asked_to_skip", "ts_queue")).strip() == "0"
    assert node2.query(series_count("asked_to_skip", "ts_queue")).strip() == "0"


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


def test_a_missing_shard_table_is_refused_for_a_write_and_never_validated():
    node2.query("RENAME TABLE ts_missing TO ts_missing_away")
    # Routed to node2, which has no `ts_missing`: refused with an error Prometheus retries, and
    # queued for nothing, where the sink would have kept the samples for whatever takes the name.
    response = write("while_missing", "/missing/write")
    assert response.status_code >= 500, response.text
    assert "ALL_CONNECTION_TRIES_FAILED" in response.text
    assert "no table" in response.text, response.text
    pending = node1.query(
        "SELECT sum(data_files) FROM system.distribution_queue WHERE table = 'prom_missing'"
    )
    assert pending.strip() == "0", pending
    # A read told to skip the shard passes, served by node1 alone.
    node1.query(
        f"SELECT count() FROM prometheusQuery(prom_missing, 'm', {START_TIME}) "
        "SETTINGS skip_unavailable_shards = 1"
    )

    # A MergeTree table answers to the name now: refused by the next write's own check.
    node2.query("RENAME TABLE mt_missing TO ts_missing")
    response = write("into_mergetree", "/missing/write")
    assert response.status_code >= 400, response.text
    assert "UNEXPECTED_TABLE_ENGINE" in response.text
    node1.query("SYSTEM FLUSH DISTRIBUTED prom_missing")
    assert node2.query("SELECT count() FROM ts_missing").strip() == "0"

    # Restored, the very next write is accepted and lands on node2.
    node2.query("RENAME TABLE ts_missing TO mt_missing, ts_missing_away TO ts_missing")
    assert write("restored_missing", "/missing/write").status_code == 204
    node1.query("SYSTEM FLUSH DISTRIBUTED prom_missing")
    assert node2.query(series_count("restored_missing", "ts_missing")).strip() == "1"
    assert node1.query(series_count("while_missing", "ts_missing")).strip() == "0"
    assert node2.query(series_count("while_missing", "ts_missing")).strip() == "0"
