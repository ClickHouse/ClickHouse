import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
replica2 = cluster.add_instance(
    "replica2", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
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


def cleanup(nodes):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS source SYNC")
        node.query("DROP TABLE IF EXISTS destination SYNC")


def create_table(node, table_name):
    replica = node.name
    engine = f"ReplicatedMergeTree('/clickhouse/tables/1/{table_name}', '{replica}')"

    node.query_with_retry(
        """
        CREATE TABLE {table_name} 
        (
            number UInt64
        ) 
        ENGINE={engine}
        ORDER BY number
        """.format(
            table_name=table_name, engine=engine
        )
    )


def test_drop_replicated(start_cluster):
    create_table(replica1, "test_table")

    replica1.query("INSERT INTO test_table SELECT number FROM system.numbers LIMIT 6;")

    replica1.query("SYSTEM SYNC REPLICA test_table;", timeout=20)

    replica1.query("DETACH TABLE test_table PERMANENTLY;")

    replica1.query("SET allow_drop_detached_table=1; DROP TABLE test_table;")

    cleanup([replica2])
