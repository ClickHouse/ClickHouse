import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Create two nodes - one main node and one replica
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_matview_union_replicated(started_cluster):

    # Create replicated database, source and target tables and matview
    node1.query("DROP DATABASE IF EXISTS union_test_replicated SYNC")
    node1.query("CREATE DATABASE union_test_replicated ENGINE=Replicated('/test/union_replica' , 'shard1', 'replica' || '1');")

    node1.query("""
    CREATE TABLE union_test_replicated.source_1
    (
        timestamp DateTime,
        value Float64
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY timestamp
    """)

    node1.query("""
    CREATE TABLE union_test_replicated.source_2
    (
        timestamp DateTime,
        value Float64
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY timestamp
    """)

    node1.query("""
    CREATE TABLE union_test_replicated.target
    (
        timestamp DateTime,
        value Float64
    )
    ENGINE = ReplicatedMergeTree
    ORDER BY timestamp
    """)
    
    node1.query("""
    CREATE MATERIALIZED VIEW union_test_replicated.mv_test TO union_test_replicated.target AS
    WITH source_data AS
    (
        SELECT timestamp, value FROM union_test_replicated.source_1
        UNION ALL
        SELECT timestamp, value FROM union_test_replicated.source_2
    )
    SELECT timestamp, value FROM source_data
    """)

    # Verify INSERT works on Node #1
    node1.query("INSERT INTO union_test_replicated.source_1 VALUES (now(), 1)")

    # Attach replica on second node
    node2.query("DROP DATABASE IF EXISTS union_test_replicated SYNC")
    node2.query("CREATE DATABASE union_test_replicated ENGINE=Replicated('/test/union_replica' , 'shard1', 'replica' || '2');")
    node2.query("SYSTEM SYNC DATABASE REPLICA union_test_replicated")

    # Verify the table structure on replica
    assert_eq_with_retry(
        node2,
        "DESCRIBE TABLE union_test_replicated.source_1",
        "timestamp\tDateTime\t\t\t\t\nvalue\tFloat64\t\t\t\t\n"
    )

    # Run INSERT on replica
    node2.query("INSERT INTO union_test_replicated.source_1 VALUES (now(), 1)")

    # Clean up
    node1.query("DROP DATABASE IF EXISTS union_test_replicated SYNC")
    node2.query("DROP DATABASE IF EXISTS union_test_replicated SYNC")
