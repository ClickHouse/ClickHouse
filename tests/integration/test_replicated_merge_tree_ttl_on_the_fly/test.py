#!/usr/bin/env python3

import pytest
from helpers.cluster import ClickHouseCluster
import time

cluster = ClickHouseCluster(__file__)

# Define two nodes that are replicas of each other
node_1_1 = cluster.add_instance(
    "node_1_1",
    main_configs=["configs/cluster.xml"],
    with_zookeeper=True,
    macros={"replica": "1", "shard": "1"},
)

node_1_2 = cluster.add_instance(
    "node_1_2",
    main_configs=["configs/cluster.xml"],
    with_zookeeper=True,
    macros={"replica": "2", "shard": "1"},
)

# Start the cluster
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_table_apply_ttl_on_fly(started_cluster):
    """
    Test that a ReplicatedMergeTree table in a Replicated database correctly applies TTL
    when using the apply_ttl_on_fly setting and that replicas remain consistent.
    """
    started_cluster.wait_zookeeper_to_start()

    # Create database (Replicated database)
    node_1_1.query("""
        CREATE DATABASE IF NOT EXISTS mydb ON CLUSTER 'cluster'
        ENGINE = Replicated('/clickhouse/path/mydb/', '{shard}', '{replica}')
    """)

    # Create table using default ReplicatedMergeTree parameters (do not pass explicit path/replica)
    node_1_1.query("""
        CREATE TABLE IF NOT EXISTS mydb.replicated_ttl_test
        (
            event_time DateTime,
            user_id UInt64,
            debug_info String,
            event_type String,
            value UInt64
        )
        ENGINE = ReplicatedMergeTree
        PARTITION BY toYYYYMM(event_time)
        ORDER BY (event_type, toDate(event_time), event_time)
        TTL event_time + toIntervalSecond(1)
        SETTINGS index_granularity = 8192;
    """)

    # Synchronize replica
    node_1_2.query("SYSTEM SYNC REPLICA mydb.replicated_ttl_test")

    # Stop background merges to prevent automatic TTL application
    node_1_1.query("SYSTEM STOP MERGES mydb.replicated_ttl_test")
    node_1_2.query("SYSTEM STOP MERGES mydb.replicated_ttl_test")

    # Insert test data into node 1
    node_1_1.query("""
        INSERT INTO mydb.replicated_ttl_test
        SELECT
            now() AS event_time,
            rand() % 1000000 AS user_id,
            concat('debug_', toString(number)) AS debug_info,
            arrayElement(['click','view','error'], rand() % 3 + 1) AS event_type,
            rand() % 1000 AS value
        FROM numbers(10)
    """)

    # Wait until the replica is synchronized
    node_1_2.query("SYSTEM SYNC REPLICA mydb.replicated_ttl_test")

    # Verify that both replicas contain the same data
    result_node1 = node_1_1.query("SELECT * FROM mydb.replicated_ttl_test")
    result_node2 = node_1_2.query("SELECT * FROM mydb.replicated_ttl_test")
    assert result_node1 == result_node2, "Data mismatch between replicas"

    # Wait for TTL to expire
    time.sleep(2)

    # With apply_ttl_on_fly = 1, both nodes should return no data
    result_node1_ttl = node_1_1.query("SELECT * FROM mydb.replicated_ttl_test SETTINGS apply_ttl_on_fly = 1")
    result_node2_ttl = node_1_2.query("SELECT * FROM mydb.replicated_ttl_test SETTINGS apply_ttl_on_fly = 1")
    assert result_node1_ttl.strip() == "" and result_node2_ttl.strip() == "", "TTL did not remove expired rows"

    # Verify data consistency again without TTL applied
    result_node1_no_ttl = node_1_1.query("SELECT * FROM mydb.replicated_ttl_test SETTINGS apply_ttl_on_fly = 0")
    result_node2_no_ttl = node_1_2.query("SELECT * FROM mydb.replicated_ttl_test SETTINGS apply_ttl_on_fly = 0")
    assert result_node1_no_ttl == result_node2_no_ttl, "Replica data mismatch without TTL"
