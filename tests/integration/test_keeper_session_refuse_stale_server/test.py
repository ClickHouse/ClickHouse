#!/usr/bin/env python3

import pytest
import random
import string

from helpers.cluster import ClickHouseCluster, ZOOKEEPER_CONTAINERS

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def randomize_table_name(table_name, random_suffix_length=10):
    letters = string.ascii_letters + string.digits
    return f"{table_name}_{''.join(random.choice(letters) for _ in range(random_suffix_length))}"


def test_session_refused_on_stale_keeper(started_cluster):
    def last_zxid_seen_metric():
        node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")
        return int(
            node.query("SELECT value FROM system.asynchronous_metrics WHERE name = 'ZooKeeperClientLastZXIDSeen'").strip()
        )

    def connection_loss_started_timestamp_metric():
        return int(
            node.query("SELECT value FROM system.metrics WHERE name = 'ZooKeeperConnectionLossStartedTimestampSeconds'").strip()
        )

    def last_zxid_seen_column():
        return int(
            node.query(
                "SELECT last_zxid_seen FROM system.zookeeper_connection WHERE name = 'default'"
            ).strip()
        )

    t = randomize_table_name("t")
    t2 = randomize_table_name("t2")

    # sanity check the value of last_zxid_seen
    node.query(f"CREATE DATABASE {t} ENGINE = Replicated('/clickhouse/databases/{t}', 's1', 'r1')")
    assert connection_loss_started_timestamp_metric() == 0
    assert 1 <= last_zxid_seen_column() <= 1000
    assert 1 <= last_zxid_seen_metric() <= 1000

    # remove all the logs and snapshots from keepers and restart them
    for zookeeper_container in ZOOKEEPER_CONTAINERS:
        zookeeper_container_id = started_cluster.get_container_id(zookeeper_container)
        started_cluster.remove_directory_from_container(
            zookeeper_container_id,
            "/var/lib/clickhouse-keeper/logs"
        )
        started_cluster.remove_directory_from_container(
            zookeeper_container_id,
            "/var/lib/clickhouse-keeper/snapshots"
        )
    started_cluster.stop_zookeeper_nodes(ZOOKEEPER_CONTAINERS)
    started_cluster.start_zookeeper_nodes(ZOOKEEPER_CONTAINERS)

    # all the connection attempts should fail because keeper client's last_zxid_seen is larger than the server's
    error = node.query_and_get_error(f"CREATE DATABASE {t2} ENGINE = Replicated('/clickhouse/databases/{t2}', 's1', 'r1')")
    assert (
        "All connection tries failed while connecting to ZooKeeper" in error
    )

    assert connection_loss_started_timestamp_metric() > 0
    assert 1 <= last_zxid_seen_metric() <= 1000

    # after restart, the keeper client's last_zxid_seen is reset to 0
    # this behaviour is not ideal, but persisting last_zxid_seen to the disk would:
    # (a) still not help with horizontal scaling and MBB releases
    # (b) introduce complexity
    node.restart_clickhouse()

    node.query(f"CREATE DATABASE {t2} ENGINE = Replicated('/clickhouse/databases/{t2}', 's1', 'r1')")

    assert connection_loss_started_timestamp_metric() == 0
    assert 1 <= last_zxid_seen_metric() <= 1000
    assert 1 <= last_zxid_seen_column() <= 1000
