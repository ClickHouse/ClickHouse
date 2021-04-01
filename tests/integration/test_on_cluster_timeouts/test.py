import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'],
                             user_configs=['configs/users_config.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'],
                             user_configs=['configs/users_config.xml'], with_zookeeper=True)
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'],
                             user_configs=['configs/users_config.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', main_configs=['configs/remote_servers.xml'],
                             user_configs=['configs/users_config.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_long_query(started_cluster):
    node1.query(
        "CREATE TABLE cluster_table (key UInt64, value String) ENGINE = ReplicatedMergeTree('/test/1/cluster_table', '1') ORDER BY tuple()")
    node2.query(
        "CREATE TABLE cluster_table (key UInt64, value String) ENGINE = ReplicatedMergeTree('/test/1/cluster_table', '2') ORDER BY tuple()")

    node1.query("INSERT INTO cluster_table SELECT number, toString(number) FROM numbers(20)")
    node2.query("SYSTEM SYNC REPLICA cluster_table")

    node3.query(
        "CREATE TABLE cluster_table (key UInt64, value String) ENGINE = ReplicatedMergeTree('/test/2/cluster_table', '1') ORDER BY tuple()")

    node4.query(
        "CREATE TABLE cluster_table (key UInt64, value String) ENGINE = ReplicatedMergeTree('/test/2/cluster_table', '2') ORDER BY tuple()")
    node3.query("INSERT INTO cluster_table SELECT number, toString(number) FROM numbers(20)")
    node4.query("SYSTEM SYNC REPLICA cluster_table")

    node1.query("ALTER TABLE cluster_table ON CLUSTER 'test_cluster' UPDATE key = 1 WHERE sleepEachRow(1) == 0",
                settings={"mutations_sync": "2"})

    assert node1.query("SELECT SUM(key) FROM cluster_table") == "20\n"
    assert node2.query("SELECT SUM(key) FROM cluster_table") == "20\n"
    assert node3.query("SELECT SUM(key) FROM cluster_table") == "20\n"
    assert node4.query("SELECT SUM(key) FROM cluster_table") == "20\n"
