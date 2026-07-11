import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_zk_connected_time(instance):
    query_zk_connected_time = (
        "SELECT connected_time FROM system.zookeeper_connection;"
    )
    return instance.query(query_zk_connected_time).strip()


def test_system_reconnect_zookeeper(start_cluster):
    instance.query("DROP TABLE IF EXISTS simple SYNC;")
    instance.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'instance') ORDER BY tuple() PARTITION BY date;"
    )

    old_zk_connected_time = get_zk_connected_time(instance)
    time.sleep(2)
    instance.query("SYSTEM RECONNECT ZOOKEEPER;")
    new_zk_connected_time = get_zk_connected_time(instance)

    assert old_zk_connected_time != new_zk_connected_time
