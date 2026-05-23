"""
Integration tests for replica_host configuration in DatabaseReplicated

This test suite validates:
1. Basic replica_host configuration usage
2. Fallback chain: replica_host → hostname
3. DDL and data replication with replica_host
4. Special character handling (URL encoding)
"""

import pytest
import urllib.parse
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Node with replica_host configured
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config_with_replica_host.xml"],
    with_zookeeper=True,
    macros={"replica": "node1", "shard": "shard1"},
    stay_alive=True,
)

# Node without replica_host (uses hostname fallback)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": "node2", "shard": "shard1"},
    stay_alive=True,
)

# Node without any custom config (uses hostname)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    macros={"replica": "node3", "shard": "shard1"},
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_replica_host_basic(started_cluster):
    """Test that replica_host configuration is used in host_id."""

    # Create DatabaseReplicated on node1 (with replica_host)
    node1.query(
        "CREATE DATABASE test_basic ENGINE = Replicated('/clickhouse/databases/test_basic', 'shard1', 'node1')"
    )

    # Wait for registration
    node1.query("SYSTEM SYNC DATABASE REPLICA test_basic")

    # Get host_id from ZooKeeper
    zk_path = "/clickhouse/databases/test_basic/replicas"
    host_ids = node1.query(
        f"SELECT value FROM system.zookeeper WHERE path = '{zk_path}'"
    )
    host_ids_decoded = urllib.parse.unquote(host_ids)

    print(f"Host IDs in ZooKeeper: {host_ids_decoded}")

    # Verify replica_host is used (configured as "public.node1.com" in config)
    assert "public.node1.com" in host_ids_decoded, \
        f"Expected 'public.node1.com' in host_id, got: {host_ids_decoded}"

    # Verify TCP port is used
    assert ":9000:" in host_ids_decoded, \
        f"Expected TCP port 9000 in host_id, got: {host_ids_decoded}"

    # Cleanup
    node1.query("DROP DATABASE test_basic SYNC")


def test_replica_host_fallback(started_cluster):
    """Test fallback: replica_host → hostname."""

    for node in [node1, node3]:
        node.query(
            f"CREATE DATABASE test_fallback ENGINE = Replicated('/clickhouse/databases/test_fallback', 'shard1', '{node.name}')"
        )
        node.query("SYSTEM SYNC DATABASE REPLICA test_fallback")

    host_ids = node1.query(
        "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/databases/test_fallback/replicas'"
    )
    host_ids_decoded = urllib.parse.unquote(host_ids)

    assert "public.node1.com" in host_ids_decoded
    assert "node3" in host_ids_decoded

    for node in [node1, node3]:
        node.query("DROP DATABASE test_fallback SYNC")


def test_replica_host_ddl_replication(started_cluster):
    """Test DDL replication with replica_host."""

    node1.query(
        "CREATE DATABASE test_ddl ENGINE = Replicated('/clickhouse/databases/test_ddl', 'shard1', 'node1')"
    )
    node2.query(
        "CREATE DATABASE test_ddl ENGINE = Replicated('/clickhouse/databases/test_ddl', 'shard1', 'node2')"
    )

    node1.query("SYSTEM SYNC DATABASE REPLICA test_ddl")
    node2.query("SYSTEM SYNC DATABASE REPLICA test_ddl")

    node1.query(
        "CREATE TABLE test_ddl.test_table (id UInt32, value String) ENGINE = ReplicatedMergeTree ORDER BY id"
    )
    node2.query("SYSTEM SYNC DATABASE REPLICA test_ddl")

    tables_on_node2 = node2.query("SHOW TABLES FROM test_ddl")
    assert "test_table" in tables_on_node2

    node1.query("INSERT INTO test_ddl.test_table VALUES (1, 'hello'), (2, 'world')")
    node2.query("SYSTEM SYNC REPLICA test_ddl.test_table")

    count = node2.query("SELECT count() FROM test_ddl.test_table").strip()
    assert count == "2"

    value = node2.query("SELECT value FROM test_ddl.test_table WHERE id = 1").strip()
    assert value == "hello"

    node1.query("DROP DATABASE test_ddl SYNC")
    node2.query("DROP DATABASE test_ddl SYNC")


def test_replica_host_special_characters(started_cluster):
    """Test special character escaping in replica_host."""

    node1.query(
        "CREATE DATABASE test_escape ENGINE = Replicated('/clickhouse/databases/test_escape', 'shard1', 'node1')"
    )
    node1.query("SYSTEM SYNC DATABASE REPLICA test_escape")

    host_id = node1.query(
        "SELECT value FROM system.zookeeper WHERE path = '/clickhouse/databases/test_escape/replicas' AND name = 'shard1|node1'"
    ).strip()

    host_id_decoded = urllib.parse.unquote(host_id)
    parts = host_id_decoded.split(':')
    assert len(parts) >= 3

    node1.query("DROP DATABASE test_escape SYNC")


def test_replica_host_multiple_databases(started_cluster):
    """Test that multiple DatabaseReplicated instances work correctly with replica_host."""

    # Create two different databases on the same nodes
    for db_name in ["test_multi_db1", "test_multi_db2"]:
        node1.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/clickhouse/databases/{db_name}', 'shard1', 'node1')"
        )
        node2.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('/clickhouse/databases/{db_name}', 'shard1', 'node2')"
        )

    # Wait for sync
    for db_name in ["test_multi_db1", "test_multi_db2"]:
        node1.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")
        node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")

    # Verify both databases are working
    for db_name in ["test_multi_db1", "test_multi_db2"]:
        # Create table
        node1.query(f"CREATE TABLE {db_name}.test_table (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id")
        node2.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")

        # Verify table replicated
        tables = node2.query(f"SHOW TABLES FROM {db_name}")
        assert "test_table" in tables

    # Cleanup
    for db_name in ["test_multi_db1", "test_multi_db2"]:
        node1.query(f"DROP DATABASE {db_name} SYNC")
        node2.query(f"DROP DATABASE {db_name} SYNC")


def test_host_id_migration_on_restart(started_cluster):
    """Test that startup succeeds and ZooKeeper is updated when host_id changes but UUID matches.

    Simulates the scenario where replica_host (or FQDN) changes between restarts:
    the replica path in ZooKeeper holds a stale hostname, but the UUID is the same.
    DatabaseReplicated should detect this, update ZooKeeper, and start normally.
    """
    db = "test_host_id_migration"
    zk_replica_path = f"/clickhouse/databases/{db}/replicas/shard1|node2"

    node2.query(
        f"CREATE DATABASE {db} ENGINE = Replicated('/clickhouse/databases/{db}', 'shard1', 'node2')"
    )
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db}")

    # Read the current host_id written by node2
    current_host_id = node2.query(
        f"SELECT value FROM system.zookeeper WHERE path = '/clickhouse/databases/{db}/replicas' AND name = 'shard1|node2'"
    ).strip()

    assert current_host_id, "host_id not found in ZooKeeper"

    # Extract UUID (last colon-separated field)
    uuid = current_host_id.rsplit(":", 1)[-1]

    # Construct a stale host_id: different hostname, same port and UUID.
    # Use the same format as Cluster::Address::toString: escapeForFileName(host) + ':' + port.
    # For a plain hostname with no special characters, this is just "hostname:port".
    stale_host_id = f"stale-old-hostname:9000:{uuid}"

    # Overwrite ZooKeeper with the stale host_id directly via kazoo
    zk = cluster.get_kazoo_client("zoo1")
    zk.start()
    zk.set(zk_replica_path, stale_host_id.encode())
    zk.stop()

    # Verify the stale value is in ZooKeeper
    stale_in_zk = node2.query(
        f"SELECT value FROM system.zookeeper WHERE path = '/clickhouse/databases/{db}/replicas' AND name = 'shard1|node2'"
    ).strip()
    assert "stale-old-hostname" in stale_in_zk, f"Stale host_id not written: {stale_in_zk}"

    # Restart node2 — startup must succeed (not throw REPLICA_ALREADY_EXISTS)
    node2.restart_clickhouse()

    # ZooKeeper should now have the updated (current) host_id
    updated_host_id = node2.query(
        f"SELECT value FROM system.zookeeper WHERE path = '/clickhouse/databases/{db}/replicas' AND name = 'shard1|node2'"
    ).strip()

    assert "stale-old-hostname" not in updated_host_id, \
        f"Stale host_id was not replaced after restart: {updated_host_id}"
    assert uuid in updated_host_id, \
        f"UUID missing from updated host_id: {updated_host_id}"

    # Database should be fully functional after restart
    node2.query(f"SYSTEM SYNC DATABASE REPLICA {db}")
    node2.query(f"DROP DATABASE {db} SYNC")


def test_replica_host_cluster_view(started_cluster):
    """Test that cluster information shows correct host_id with replica_host."""

    node1.query(
        "CREATE DATABASE test_cluster ENGINE = Replicated('/clickhouse/databases/test_cluster', 'shard1', 'node1')"
    )
    node2.query(
        "CREATE DATABASE test_cluster ENGINE = Replicated('/clickhouse/databases/test_cluster', 'shard1', 'node2')"
    )

    node1.query("SYSTEM SYNC DATABASE REPLICA test_cluster")
    node2.query("SYSTEM SYNC DATABASE REPLICA test_cluster")

    # Query system.clusters to see cluster information
    # DatabaseReplicated creates a cluster with the same name as the database
    cluster_info = node1.query(
        "SELECT cluster, shard_num, replica_num, host_name FROM system.clusters WHERE cluster = 'test_cluster' ORDER BY replica_num"
    )
    print(f"Cluster info from node1: {cluster_info}")

    # Should see cluster named 'test_cluster' with replicas
    assert "test_cluster" in cluster_info, f"Expected cluster 'test_cluster', got: {cluster_info}"

    # Verify we have at least 2 replicas (node1 and node2)
    lines = [line for line in cluster_info.strip().split('\n') if line]
    assert len(lines) >= 2, f"Expected at least 2 replicas, got {len(lines)}: {cluster_info}"

    # Cleanup
    node1.query("DROP DATABASE test_cluster SYNC")
    node2.query("DROP DATABASE test_cluster SYNC")
