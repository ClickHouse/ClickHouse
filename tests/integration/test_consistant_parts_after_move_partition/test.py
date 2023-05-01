import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

CLICKHOUSE_DATABASE = "test"


def initialize_database(nodes, shard):
    for node in nodes:
        node.query(
            """
            CREATE DATABASE {database};
            CREATE TABLE `{database}`.src (p UInt64, d UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/test_consistent_shard1{shard}/replicated', '{replica}')
            ORDER BY d PARTITION BY p
            SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
            CREATE TABLE `{database}`.dest (p UInt64, d UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/{database}/tables/test_consistent_shard2{shard}/replicated', '{replica}')
            ORDER BY d PARTITION BY p
            SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
        """.format(
                shard=shard, replica=node.name, database=CLICKHOUSE_DATABASE
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        initialize_database([node1, node2], 1)
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test_consistent_part_after_move_partition(start_cluster):
    # insert into all replicas
    for i in range(100):
        node1.query(
            "INSERT INTO `{database}`.src VALUES ({value} % 2, {value})".format(
                database=CLICKHOUSE_DATABASE, value=i
            )
        )
    query_source = "SELECT COUNT(*) FROM `{database}`.src".format(
        database=CLICKHOUSE_DATABASE
    )
    query_dest = "SELECT COUNT(*) FROM `{database}`.dest".format(
        database=CLICKHOUSE_DATABASE
    )
    assert_eq_with_retry(node2, query_source, node1.query(query_source))
    assert_eq_with_retry(node2, query_dest, node1.query(query_dest))

    node1.query(
        "ALTER TABLE `{database}`.src MOVE PARTITION 1 TO TABLE `{database}`.dest".format(
            database=CLICKHOUSE_DATABASE
        )
    )

    assert_eq_with_retry(node2, query_source, node1.query(query_source))
    assert_eq_with_retry(node2, query_dest, node1.query(query_dest))
