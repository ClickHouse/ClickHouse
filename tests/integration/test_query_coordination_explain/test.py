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
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 1}, )
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 2}, )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """
            CREATE TABLE test_explain ON CLUSTER test_two_shards (type String, click UInt32) 
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_explain', '{replica}') 
            ORDER BY type SETTINGS index_granularity=100;
            """
        )

        node1.query(
            """
            CREATE TABLE test_explain_all ON CLUSTER test_two_shards (type String, click UInt32) 
            ENGINE = Distributed(test_two_shards, default, test_explain, rand());
            """
        )

        node1.query("INSERT INTO test_explain_all SELECT toString(number % 10), number FROM numbers(600)")
        node1.query("SYSTEM FLUSH DISTRIBUTED test_explain_all")

        yield cluster

    finally:
        cluster.shutdown()


def test_explain(started_cluster):
    # Queries with local table will not go with query_coordination(new queryPlan).
    res = node1.query(
        """
        SELECT count() > 0 
        FROM 
            (EXPLAIN SELECT * FROM test_explain SETTINGS allow_experimental_query_coordination = 1) 
        WHERE explain LIKE '%ExchangeData%'
        """
    )
    print(res)
    assert res == '0\n'

    # Queries without query coordination will go with old QueryPlan
    res = node1.query(
        """
        SELECT count() > 0 
        FROM 
            (EXPLAIN SELECT * FROM test_explain_all SETTINGS allow_experimental_query_coordination = 0) 
        WHERE explain LIKE '%ExchangeData%'
        """
    )
    print(res)
    assert res == '0\n'

    # Queries with query coordination will go with new QueryPlan
    # 'ExchangeData' only exists in new QueryPlan
    res = node1.query(
        """
        SELECT count() > 0 
        FROM 
            (EXPLAIN SELECT * FROM test_explain_all WHERE click > 3 SETTINGS allow_experimental_query_coordination = 1) 
        WHERE explain LIKE '%ExchangeData%'
        """
    )
    print(res)
    assert res == '1\n'


def test_explain_fragment(started_cluster):
    # Explain plan with query coordination does not support Json format.
    assert node1.query_and_get_error(
        "EXPLAIN FRAGMENT SELECT * FROM test_explain_all SETTINGS allow_experimental_query_coordination = 0")

    # Queries with local table will not go with query_coordination(new queryPlan).
    assert node1.query_and_get_error(
        "EXPLAIN FRAGMENT SELECT * FROM test_explain SETTINGS allow_experimental_query_coordination = 1")

    # Queries without query coordination will go with old QueryPlan
    assert node1.query_and_get_error(
        """
        SELECT count() > 0 
        FROM 
            (EXPLAIN FRAGMENT SELECT * FROM test_explain_all WHERE click > 3 SETTINGS allow_experimental_query_coordination = 0) 
        WHERE explain LIKE '%Fragment%'
        """
    )

    # Queries with query coordination will go with new QueryPlan
    res = node1.query(
        """
        SELECT count() > 0 
        FROM 
            (EXPLAIN FRAGMENT SELECT * FROM test_explain_all WHERE click > 3 SETTINGS allow_experimental_query_coordination = 1) 
        WHERE explain LIKE '%Fragment%'
        """
    )
    print(res)
    assert res == '1\n'
