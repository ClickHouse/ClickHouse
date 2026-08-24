import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query(
                """CREATE TABLE local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id;"""
            )

        node1.query(
            """CREATE TABLE distributed_table(id UInt32, val String) ENGINE = Distributed(test_cluster, default, local_table, id);"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_start_and_stop_replica_send(started_cluster):
    node1.query("SYSTEM STOP DISTRIBUTED SENDS distributed_table;")

    node1.query("INSERT INTO distributed_table VALUES (0, 'node1')")
    node1.query("INSERT INTO distributed_table VALUES (1, 'node2')")

    # Write only to this node when stop distributed sends
    assert node1.query("SELECT COUNT() FROM distributed_table").rstrip() == "1"

    node1.query("SYSTEM START DISTRIBUTED SENDS distributed_table;")
    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table;")
    assert node1.query("SELECT COUNT() FROM distributed_table").rstrip() == "2"
