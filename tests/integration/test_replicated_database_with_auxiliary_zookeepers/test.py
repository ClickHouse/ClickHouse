import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/auxiliary_zookeepers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/auxiliary_zookeepers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def drop_database(nodes, database_name):
    for node in nodes:
        node.query("DROP DATABASE IF EXISTS {} SYNC".format(database_name))

def check_table_exists(node, database_name, table_name):
    assert node.query(f"EXISTS TABLE {database_name}.{table_name}") == "1\n"


def check_table_not_exists(node, database_name, table_name):
    assert node.query(f"EXISTS TABLE {database_name}.{table_name}") == "0\n"

# Create table with auxiliary zookeeper.
def test_create_replicated_database_with_auxiliary_zookeeper(started_cluster):
    drop_database([node1, node2], "test_auxiliary_zookeeper")
    for node in [node1, node2]:
        node.query(
            """
                CREATE DATABASE test_auxiliary_zookeeper
                ENGINE = Replicated('zookeeper2:/clickhouse/databases/test_auxiliary_zookeeper', '{shard}', '{replica}')
            """)
    node1.query("CREATE TABLE test_auxiliary_zookeeper.test_table(a Int32) ENGINE = MergeTree() ORDER BY a")
    for node in [node1, node2]:
        check_table_exists(node, "test_auxiliary_zookeeper", "test_table")
    node2.query("DROP TABLE test_auxiliary_zookeeper.test_table SYNC")
    for node in [node1, node2]:
        check_table_not_exists(node, "test_auxiliary_zookeeper", "test_table")

# Create table with auxiliary zookeeper that does not exist.
def test_create_replicated_database_with_nonexistent_zookeeper(
    started_cluster,
):
    drop_database([node1], "test_not_exist_zookeeper")
    with pytest.raises(QueryRuntimeException):
        node1.query(
            """
                CREATE DATABASE test_not_exist_zookeeper
                ENGINE = Replicated('zookeeper_not_exists:/clickhouse/databases/test_not_exist_zookeeper', '{shard}', '{replica}')
            """)

