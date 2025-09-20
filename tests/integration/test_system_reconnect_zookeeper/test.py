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


def test_system_reconnect_zookeeper(start_cluster):
    instance.query(
        "CREATE TABLE simple (date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', 'instance') ORDER BY tuple() PARTITION BY date;"
    )

    instance.query("SYSTEM RECONNECT ZOOKEEPER")
    assert instance.contains_in_log("ZooKeeper connection closed")
