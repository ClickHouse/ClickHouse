#!/usr/bin/env python3

import pytest
import time

import helpers.keeper_utils as keeper_utils
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


def test_session_refused_on_stale_keeper(started_cluster):
    node.query("DROP DATABASE IF EXISTS t SYNC")
    node.query("DROP DATABASE IF EXISTS t2 SYNC")

    # sanity check the value of last_zxid_seen
    node.query("CREATE DATABASE t ENGINE = Replicated('/clickhouse/databases/t', 's1', 'r1')")
    last_zxid_seen_column = int(
        node.query(
            "SELECT last_zxid_seen FROM system.zookeeper_connection WHERE name = 'default'"
        ).strip()
    )
    assert 1 <= last_zxid_seen_column <= 1000

    last_zxid_seen_metric = int(
        node.query("SELECT value FROM system.asynchronous_metrics WHERE name = 'ZooKeeperClientLastZXIDSeen'").strip()
    )
    assert 1 <= last_zxid_seen_metric <= 1000

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
    error = node.query_and_get_error("CREATE DATABASE t2 ENGINE = Replicated('/clickhouse/databases/t2', 's1', 'r1')")
    assert (
        "All connection tries failed while connecting to ZooKeeper" in error
    )

    node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS")

    # make sure ZooKeeperClientLastZXIDSeen is still correct when keeper session is expired
    last_zxid_seen_metric = int(
        node.query("SELECT value FROM system.asynchronous_metrics WHERE name = 'ZooKeeperClientLastZXIDSeen'").strip()
    )
    assert 1 <= last_zxid_seen_metric <= 1000

    connection_loss_metric = int(
        node.query("SELECT value FROM system.asynchronous_metrics WHERE name = 'ZooKeeperClientConnectionLoss'").strip()
    )
    assert connection_loss_metric == 1

    # after restart, the keeper client's last_zxid_seen is reset to 0
    # this behaviour is not ideal, but persisting last_zxid_seen to the disk would:
    # (a) still not help with horizontal scaling and MBB releases
    # (b) introduce complexity
    node.restart_clickhouse()
    node.query("CREATE DATABASE t2 ENGINE = Replicated('/clickhouse/databases/t2', 's1', 'r1')")

    connection_loss_metric = int(
        node.query("SELECT value FROM system.asynchronous_metrics WHERE name = 'ZooKeeperClientConnectionLoss'").strip()
    )
    assert connection_loss_metric == 0

    node.query("DROP DATABASE t SYNC")
    node.query("DROP DATABASE t2 SYNC")
