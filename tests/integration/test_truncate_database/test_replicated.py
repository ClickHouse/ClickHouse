import pytest

from helpers.cluster import ClickHouseCluster


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                DROP DATABASE IF EXISTS test;
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

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_truncate_database_replicated(start_cluster):
    fill_nodes([node1, node2, node3], 1)

    node1.query(
        "INSERT INTO test.test_table SELECT number, toString(number) FROM numbers(100)"
    )
    assert node2.query("SELECT min(id) FROM test.test_table") == "0\n"
    assert node2.query("SELECT id FROM test.test_table ORDER BY id LIMIT 1") == "0\n"
    assert node3.query("SHOW DATABASES LIKE 'test'") == "test\n"
    node3.query("TRUNCATE DATABASE test ON CLUSTER test_cluster SYNC")
    assert node2.query("SHOW TABLES FROM test") == ""
