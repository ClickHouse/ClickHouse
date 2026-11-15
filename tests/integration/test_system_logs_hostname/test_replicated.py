import pytest

from helpers.cluster import ClickHouseCluster


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                CREATE DATABASE test;
    
                CREATE TABLE test.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date);
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", with_zookeeper=True, main_configs=["configs/replicated_servers.xml"]
)
node2 = cluster.add_instance(
    "node2", with_zookeeper=True, main_configs=["configs/replicated_servers.xml"]
)
node3 = cluster.add_instance(
    "node3", with_zookeeper=True, main_configs=["configs/replicated_servers.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node1, node2, node3], 1)

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_truncate_database_replicated(start_cluster):
    node1.query("SELECT 1", query_id="query_node1")
    node2.query("SELECT 1", query_id="query_node2")
    node3.query("SELECT 1", query_id="query_node3")
    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")
    node3.query("SYSTEM FLUSH LOGS")
    assert (
        node1.query(
            "SELECT hostname from system.query_log  where query_id='query_node1' LIMIT 1"
        )
        == "node1\n"
    )
    assert (
        node2.query(
            "SELECT hostname from system.query_log  where query_id='query_node2' LIMIT 1"
        )
        == "node2\n"
    )
    assert (
        node3.query(
            "SELECT hostname from system.query_log  where query_id='query_node3' LIMIT 1"
        )
        == "node3\n"
    )
