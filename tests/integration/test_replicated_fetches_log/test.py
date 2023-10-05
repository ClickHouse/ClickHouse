#!/usr/bin/env python3
from helpers.cluster import ClickHouseCluster
import pytest
import time

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/replicated_fetches_log.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/replicated_fetches_log.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_fetch_log(start_cluster):
    print("Limited fetches single table")
    try:
        for i, node in enumerate([node1, node2]):
            node.query(
                f"CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '{i}') ORDER BY tuple() PARTITION BY key SETTINGS max_replicated_fetches_network_bandwidth=10485760"
            )

        for i in range(3):
            node1.query("INSERT INTO t SELECT {}, 'foo' FROM numbers(300)".format(i))

        time.sleep(2)

        node2.query("SYSTEM FLUSH LOGS")

        count = int(node2.query("SELECT count() FROM system.replicated_fetches_log"))
        assert (
            count > 0,
            "System table replicated_fetches_log should have records after fetching, but it's empty",
        )

    finally:
        for node in [node1, node2]:
            node.query("DROP TABLE IF EXISTS limited_fetch_table SYNC")
