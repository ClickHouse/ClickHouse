#!/usr/bin/env python3

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

# Disable `with_remote_database_disk` as the test does not use the default Keeper.
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/keeper_config.xml",
        "configs/enable_keeper1.xml",
    ],
    macros={"replica": "node1"},
    with_remote_database_disk=False,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/zookeeper_config.xml",
        "configs/enable_keeper2.xml",
    ],
    macros={"replica": "node2"},
    with_remote_database_disk=False,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/remote_servers.xml", "configs/enable_keeper3.xml"],
    macros={"replica": "node3"},
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create_insert(started_cluster):
    node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'test_cluster' SYNC")
    node1.query(
        """
        CREATE TABLE tbl ON CLUSTER 'test_cluster' (
            id Int64,
            str String
        ) ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')
        ORDER BY id
        """
    )

    node1.query("INSERT INTO tbl VALUES (1, 'str1')")
    node2.query("INSERT INTO tbl VALUES (1, 'str1')")  # Test deduplication
    node3.query("INSERT INTO tbl VALUES (2, 'str2')")

    node1.query("SYSTEM SYNC REPLICA ON CLUSTER 'test_cluster' tbl")

    for node in [node1, node2, node3]:
        expected = [[1, "str1"], [2, "str2"]]
        assert node.query("SELECT * FROM tbl ORDER BY id") == TSV(expected)
        assert node.query("CHECK TABLE tbl") == "1\n"
