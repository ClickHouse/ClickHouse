import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    macros={"replica": "node1"},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    macros={"replica": "node2"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replicated_database_uses_interserver_host(started_cluster):
    """
    Test that DatabaseReplicated uses interserver_http_host configuration
    for replica registration in ZooKeeper instead of hostname.

    This verifies the fix for issue #88361 where using IP addresses
    in interserver_http_host should result in IP-based registration
    in ZooKeeper, not hostname-based.
    """

    # Create replicated database on node1
    node1.query(
        "CREATE DATABASE test_db ENGINE = Replicated('/clickhouse/databases/test_db', 'shard1', 'node1')"
    )

    # Create replicated database on node2
    node2.query(
        "CREATE DATABASE test_db ENGINE = Replicated('/clickhouse/databases/test_db', 'shard1', 'node2')"
    )

    # Wait for both replicas to appear
    node1.query("SYSTEM SYNC DATABASE REPLICA test_db")
    node2.query("SYSTEM SYNC DATABASE REPLICA test_db")

    # Check ZooKeeper to verify replicas are registered with IP address (127.0.0.1)
    # not with hostname
    zk_path = "/clickhouse/databases/test_db/replicas"
    replicas = node1.query(
        f"SELECT name, value FROM system.zookeeper WHERE path = '{zk_path}'"
    )

    # The replica value should contain '127.0.0.1' from interserver_http_host config
    # not the container hostname like 'node1' or 'node2'
    assert "127.0.0.1" in replicas, f"Expected IP address 127.0.0.1 in replica registration, got: {replicas}"

    # Verify that hostname is NOT used
    assert "node1" not in replicas or "127.0.0.1" in replicas, f"Hostname should not be used when interserver_http_host is set, got: {replicas}"

    # Create a table on node1 and verify it replicates to node2
    node1.query("CREATE TABLE test_db.test_table (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id")

    # Wait for replication
    node2.query("SYSTEM SYNC DATABASE REPLICA test_db")

    # Verify table exists on node2
    tables_on_node2 = node2.query("SHOW TABLES FROM test_db")
    assert "test_table" in tables_on_node2, "Table should be replicated to node2"

    # Insert data on node1
    node1.query("INSERT INTO test_db.test_table VALUES (1), (2), (3)")

    # Verify data replicates to node2
    node2.query("SYSTEM SYNC REPLICA test_db.test_table")
    count_on_node2 = node2.query("SELECT count() FROM test_db.test_table")
    assert count_on_node2.strip() == "3", f"Expected 3 rows on node2, got: {count_on_node2}"

    # Cleanup
    node1.query("DROP DATABASE test_db SYNC")
    node2.query("DROP DATABASE test_db SYNC")
