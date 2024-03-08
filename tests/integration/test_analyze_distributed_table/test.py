import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 1}, )
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 2}, )
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 1}, )
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 2}, )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """
            CREATE TABLE test_analyze_distributed_table ON CLUSTER test_two_shards (id UInt32) 
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_analyze_distributed_table', '{replica}') 
            ORDER BY id;
            """
        )

        node1.query(
            """
            CREATE TABLE test_analyze_distributed_table_all ON CLUSTER test_two_shards (id UInt32) 
            ENGINE = Distributed(test_two_shards, default, test_analyze_distributed_table, rand());
            """
        )

        node1.query("INSERT INTO test_analyze_distributed_table SELECT number FROM numbers(5) SETTINGS insert_quorum=2")
        node3.query("INSERT INTO test_analyze_distributed_table SELECT number FROM numbers(5, 5) SETTINGS insert_quorum=2")

        yield cluster

    finally:
        cluster.shutdown()


def test_analyze_distributed_table(started_cluster):
    node1.query("ANALYZE TABLE test_analyze_distributed_table_all")

    row_count = node1.query("SELECT sum(row_count) FROM cluster('test_two_shards', 'system', 'statistics_table')")
    assert row_count == '10\n'

    column_result = node1.query(
        """
        SELECT `table`, `column`, uniqMerge(ndv), min(min_value), max(max_value), avg(avg_row_size)
        FROM cluster('test_two_shards', 'system', 'statistics_column_basic')
        GROUP BY `table`, `column`
        """
    )
    print(column_result)
    assert column_result == 'test_analyze_distributed_table\tid\t10\t0\t9\t8\n'
