import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """
CREATE TABLE local_table
(
    `ID` UInt32,
    `Name` String
)
ENGINE = MergeTree
ORDER BY ID;
"""
        )

        node2.query(
            """
CREATE TABLE local_table
(
    `ID` UInt32
)
ENGINE = MergeTree
ORDER BY ID;
"""
        )

        node3.query(
            """
CREATE TABLE distributed_table_unavailable_or_exception
(
    `ID` UInt32,
    `Name` String
)
ENGINE = Distributed(test_cluster, default, local_table, rand())
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode='unavailable_or_exception';
"""
        )

        node3.query(
            """
CREATE TABLE distributed_table_unavailable
(
    `ID` UInt32,
    `Name` String
)
ENGINE = Distributed(test_cluster, default, local_table, rand())
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode='unavailable';
"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_start_and_stop_replica_send(started_cluster):
    node1.query("INSERT INTO local_table VALUES (0, 'node1')")
    node2.query("INSERT INTO local_table VALUES (1)")

    assert node3.query("SELECT Name FROM distributed_table_unavailable_or_exception").rstrip() == "2"
    assert node3.query("SELECT Name FROM distributed_table_unavailable SETTINGS skip_unavailable_shards_mode='unavailable_or_exception'").rstrip() == "2"

