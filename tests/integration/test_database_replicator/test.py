
"""
Integration tests for DatabaseReplicator.

Tests cover:
- Sync CREATE DATABASE across nodes
- Sync DROP DATABASE across nodes
- Sync RENAME DATABASE across nodes
- Sync ALTER DATABASE (modify comment, modify Replicated engine parameters) across nodes
- New node automatically syncs existing databases on startup
- DETACH DATABASE is prohibited when DatabaseReplicator is enabled
"""

import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "node1"},
    stay_alive=True,
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "node2"},
    stay_alive=True,
    with_zookeeper=True,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "node3"},
    stay_alive=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_databases(node, exclude_system=True):
    """Return a sorted list of database names on the given node."""
    result = node.query("SELECT name FROM system.databases ORDER BY name").strip()
    databases = [db for db in result.split("\n") if db]
    if exclude_system:
        system_dbs = {
            "INFORMATION_SCHEMA",
            "information_schema",
            "system",
            "default",
        }
        databases = [db for db in databases if db not in system_dbs]
    return sorted(databases)


def get_database_comment(node, db_name):
    """Return the comment of a database."""
    return node.query(
        f"SELECT comment FROM system.databases WHERE name = '{db_name}'"
    ).strip()


def get_database_engine(node, db_name):
    """Return the engine name of a database."""
    return node.query(
        f"SELECT engine FROM system.databases WHERE name = '{db_name}'"
    ).strip()


def get_database_engine_full(node, db_name):
    """Return the full engine expression of a database."""
    return node.query(
        f"SELECT engine_full FROM system.databases WHERE name = '{db_name}'"
    ).strip()


def database_exists(node, db_name):
    """Return True if the given database exists on the node."""
    result = node.query(
        f"SELECT count() FROM system.databases WHERE name = '{db_name}'"
    ).strip()
    return result == "1"


def wait_for_database(node, db_name, exists=True, retry_count=30, sleep_time=1):
    """Wait until a database exists (or is removed) on the given node."""
    for _ in range(retry_count):
        if database_exists(node, db_name) == exists:
            return
        time.sleep(sleep_time)
    actual = "exists" if database_exists(node, db_name) else "does not exist"
    expected = "exist" if exists else "not exist"
    raise AssertionError(
        f"Timed out waiting for database '{db_name}' to {expected} on {node.name} (actual: {actual})"
    )


# ---------------------------------------------------------------------------
# Test: CREATE DATABASE replication
# ---------------------------------------------------------------------------
def test_create_database_sync(started_cluster):
    """CREATE DATABASE on node1 should be replicated to node2 and node3."""
    db_name = "test_create_db_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_name}")

        # Wait for replication
        wait_for_database(node2, db_name, exists=True)
        wait_for_database(node3, db_name, exists=True)

        # Verify all nodes have the database
        assert database_exists(node1, db_name)
        assert database_exists(node2, db_name)
        assert database_exists(node3, db_name)

        # Verify the engine is the same on all nodes
        engine1 = get_database_engine(node1, db_name)
        engine2 = get_database_engine(node2, db_name)
        engine3 = get_database_engine(node3, db_name)
        assert engine1 == engine2 == engine3

    finally:
        node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


def test_create_database_with_engine_sync(started_cluster):
    """CREATE DATABASE with Replicated engine should be replicated to all nodes."""
    db_name = "test_create_repl_db_" + uuid.uuid4().hex[:8]
    db_uuid = uuid.uuid4()

    try:
        node1.query(
            f"CREATE DATABASE {db_name} UUID '{db_uuid}' "
            f"ENGINE = Replicated('/clickhouse/databases/{db_name}', '{{shard}}', '{{replica}}')"
        )

        wait_for_database(node2, db_name, exists=True)
        wait_for_database(node3, db_name, exists=True)

        assert database_exists(node1, db_name)
        assert database_exists(node2, db_name)
        assert database_exists(node3, db_name)

        # All nodes should have the same Replicated engine
        for node in [node1, node2, node3]:
            assert get_database_engine(node, db_name) == "Replicated"

    finally:
        for node in [node1, node2, node3]:
            node.query(
                f"DROP DATABASE IF EXISTS {db_name} SYNC",
                settings={"distributed_ddl_task_timeout": 10},
            )


def test_create_database_with_comment_sync(started_cluster):
    """CREATE DATABASE with a comment should replicate the comment to all nodes."""
    db_name = "test_create_comment_db_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_name} COMMENT 'my test comment'")

        wait_for_database(node2, db_name, exists=True)
        wait_for_database(node3, db_name, exists=True)

        for node in [node1, node2, node3]:
            assert get_database_comment(node, db_name) == "my test comment"

    finally:
        node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


# ---------------------------------------------------------------------------
# Test: DROP DATABASE replication
# ---------------------------------------------------------------------------
def test_drop_database_sync(started_cluster):
    """DROP DATABASE on node1 should be replicated to node2 and node3."""
    db_name = "test_drop_db_" + uuid.uuid4().hex[:8]

    # First create
    node1.query(f"CREATE DATABASE {db_name}")
    wait_for_database(node2, db_name, exists=True)
    wait_for_database(node3, db_name, exists=True)

    # Now drop
    node1.query(f"DROP DATABASE {db_name} SYNC")

    wait_for_database(node1, db_name, exists=False)
    wait_for_database(node2, db_name, exists=False)
    wait_for_database(node3, db_name, exists=False)

    assert not database_exists(node1, db_name)
    assert not database_exists(node2, db_name)
    assert not database_exists(node3, db_name)


def test_drop_database_if_exists_sync(started_cluster):
    """DROP DATABASE IF EXISTS should be replicated without error even if db is already gone."""
    db_name = "test_drop_if_exists_db_" + uuid.uuid4().hex[:8]

    node1.query(f"CREATE DATABASE {db_name}")
    wait_for_database(node2, db_name, exists=True)

    node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
    wait_for_database(node2, db_name, exists=False)
    wait_for_database(node3, db_name, exists=False)


# ---------------------------------------------------------------------------
# Test: RENAME DATABASE replication
# ---------------------------------------------------------------------------
def test_rename_database_sync(started_cluster):
    """RENAME DATABASE on node1 should be replicated to all nodes."""
    db_name = "test_rename_db_" + uuid.uuid4().hex[:8]
    db_name_new = db_name + "_renamed"

    try:
        node1.query(f"CREATE DATABASE {db_name}")
        wait_for_database(node2, db_name, exists=True)
        wait_for_database(node3, db_name, exists=True)

        node1.query(f"RENAME DATABASE {db_name} TO {db_name_new}")

        wait_for_database(node2, db_name_new, exists=True)
        wait_for_database(node3, db_name_new, exists=True)
        wait_for_database(node2, db_name, exists=False)
        wait_for_database(node3, db_name, exists=False)

        for node in [node1, node2, node3]:
            assert database_exists(node, db_name_new)
            assert not database_exists(node, db_name)

    finally:
        for db in [db_name, db_name_new]:
            node1.query(f"DROP DATABASE IF EXISTS {db} SYNC")


# ---------------------------------------------------------------------------
# Test: ALTER DATABASE replication
# ---------------------------------------------------------------------------
def test_alter_database_comment_sync(started_cluster):
    """ALTER DATABASE ... MODIFY COMMENT should be replicated to all nodes."""
    db_name = "test_alter_comment_db_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_name} COMMENT 'initial comment'")
        wait_for_database(node2, db_name, exists=True)
        wait_for_database(node3, db_name, exists=True)

        # Verify initial comment
        for node in [node1, node2, node3]:
            assert get_database_comment(node, db_name) == "initial comment"

        # Alter comment
        node1.query(f"ALTER DATABASE {db_name} MODIFY COMMENT 'updated comment'")

        # Wait for replication of the alter
        for node in [node2, node3]:
            assert_eq_with_retry(
                node,
                f"SELECT comment FROM system.databases WHERE name = '{db_name}'",
                "updated comment\n",
                retry_count=30,
                sleep_time=1,
            )

        for node in [node1, node2, node3]:
            assert get_database_comment(node, db_name) == "updated comment"

    finally:
        node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


# ---------------------------------------------------------------------------
# Test: DETACH DATABASE is prohibited
# ---------------------------------------------------------------------------
def test_detach_database_prohibited(started_cluster):
    """DETACH DATABASE should be rejected when DatabaseReplicator is enabled."""
    db_name = "test_detach_prohibited_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_name}")
        wait_for_database(node2, db_name, exists=True)

        error = node1.query_and_get_error(f"DETACH DATABASE {db_name}")
        assert "NOT_IMPLEMENTED" in error or "DETACH DATABASE is not allowed" in error

        # Database should still exist
        assert database_exists(node1, db_name)

    finally:
        node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


# ---------------------------------------------------------------------------
# Test: New node auto-sync
# ---------------------------------------------------------------------------
def test_new_node_sync_databases_on_startup(started_cluster):
    """
    When a node restarts (simulating a new node joining), it should automatically
    recover and sync all databases from ZooKeeper metadata.

    Steps:
    1. Create several databases on node1 while node3 is running.
    2. Stop node3.
    3. Create more databases on node1 (with timeout=0 since node3 is offline).
    4. Restart node3.
    5. Verify all databases are synced to node3 after recovery.
    """
    dbs = []
    dbs_before_restart = []
    dbs_after_restart = []

    try:
        # Phase 1: Create databases while all nodes are up
        for i in range(3):
            db_name = f"test_sync_before_{uuid.uuid4().hex[:8]}"
            node1.query(f"CREATE DATABASE {db_name}")
            dbs.append(db_name)
            dbs_before_restart.append(db_name)

        for db_name in dbs_before_restart:
            wait_for_database(node3, db_name, exists=True)

        # Phase 2: Stop node3
        node3.stop_clickhouse()

        # Phase 3: Create more databases while node3 is down.
        # Use distributed_ddl_task_timeout=0 so the DDL returns immediately
        # without waiting for the offline node3.
        for i in range(2):
            db_name = f"test_sync_after_{uuid.uuid4().hex[:8]}"
            node1.query(
                f"CREATE DATABASE {db_name}",
                settings={"distributed_ddl_task_timeout": 0},
            )
            dbs.append(db_name)
            dbs_after_restart.append(db_name)

        # Wait for node2 to get them
        for db_name in dbs_after_restart:
            wait_for_database(node2, db_name, exists=True)

        # Phase 4: Start node3
        node3.start_clickhouse()

        # Phase 5: Verify all databases are synced to node3
        for db_name in dbs:
            wait_for_database(node3, db_name, exists=True)

        for db_name in dbs:
            assert database_exists(node3, db_name), f"Database {db_name} not found on node3"

    finally:
        for db_name in dbs:
            node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


# ---------------------------------------------------------------------------
# Test: Rename with mixed replicable/non-replicable is an error
# ---------------------------------------------------------------------------
def test_rename_mixed_replicable_error(started_cluster):
    """
    RENAME DATABASE from a replicable db to a non-replicable name (like 'default')
    should produce an error since one is replicable and the other is not.

    Note: 'default' is excluded from replication by canReplicateDatabase().
    """
    db_name = "test_rename_mixed_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_name}")
        wait_for_database(node2, db_name, exists=True)

        # Renaming to 'default' should fail because default is not replicable
        error = node1.query_and_get_error(f"RENAME DATABASE {db_name} TO default")
        assert "INCORRECT_QUERY" in error or "both databases must be replicable" in error

    finally:
        node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


# ---------------------------------------------------------------------------
# Test: Multiple operations in sequence
# ---------------------------------------------------------------------------
def test_multiple_operations_sequence(started_cluster):
    """
    Verify that a sequence of CREATE, ALTER, RENAME, DROP operations are
    correctly replicated across all nodes.
    """
    db1 = "test_seq_db1_" + uuid.uuid4().hex[:8]
    db2 = "test_seq_db2_" + uuid.uuid4().hex[:8]
    db1_renamed = db1 + "_renamed"

    try:
        # Step 1: Create db1
        node1.query(f"CREATE DATABASE {db1} COMMENT 'step1'")
        wait_for_database(node2, db1, exists=True)
        wait_for_database(node3, db1, exists=True)

        # Step 2: Alter comment
        node1.query(f"ALTER DATABASE {db1} MODIFY COMMENT 'step2'")
        for node in [node2, node3]:
            assert_eq_with_retry(
                node,
                f"SELECT comment FROM system.databases WHERE name = '{db1}'",
                "step2\n",
                retry_count=30,
                sleep_time=1,
            )

        # Step 3: Rename db1 -> db1_renamed
        node1.query(f"RENAME DATABASE {db1} TO {db1_renamed}")
        wait_for_database(node2, db1_renamed, exists=True)
        wait_for_database(node3, db1_renamed, exists=True)
        wait_for_database(node2, db1, exists=False)
        wait_for_database(node3, db1, exists=False)

        # Comment should still be 'step2' under the new name
        for node in [node1, node2, node3]:
            assert get_database_comment(node, db1_renamed) == "step2"

        # Step 4: Create db2
        node1.query(f"CREATE DATABASE {db2} COMMENT 'another'")
        wait_for_database(node2, db2, exists=True)
        wait_for_database(node3, db2, exists=True)

        # Step 5: Drop db1_renamed
        node1.query(f"DROP DATABASE {db1_renamed} SYNC")
        wait_for_database(node2, db1_renamed, exists=False)
        wait_for_database(node3, db1_renamed, exists=False)

        # Step 6: Drop db2
        node1.query(f"DROP DATABASE {db2} SYNC")
        wait_for_database(node2, db2, exists=False)
        wait_for_database(node3, db2, exists=False)

    finally:
        for db in [db1, db2, db1_renamed]:
            node1.query(f"DROP DATABASE IF EXISTS {db} SYNC")


# ---------------------------------------------------------------------------
# Test: DDL from different nodes
# ---------------------------------------------------------------------------
def test_create_from_different_nodes(started_cluster):
    """CREATE DATABASE issued from different nodes should all be replicated."""
    db_from_node1 = "test_from_node1_" + uuid.uuid4().hex[:8]
    db_from_node2 = "test_from_node2_" + uuid.uuid4().hex[:8]
    db_from_node3 = "test_from_node3_" + uuid.uuid4().hex[:8]

    try:
        node1.query(f"CREATE DATABASE {db_from_node1}")
        node2.query(f"CREATE DATABASE {db_from_node2}")
        node3.query(f"CREATE DATABASE {db_from_node3}")

        # All databases should appear on all nodes
        for db_name in [db_from_node1, db_from_node2, db_from_node3]:
            for node in [node1, node2, node3]:
                wait_for_database(node, db_name, exists=True)

        for db_name in [db_from_node1, db_from_node2, db_from_node3]:
            for node in [node1, node2, node3]:
                assert database_exists(node, db_name)

    finally:
        for db_name in [db_from_node1, db_from_node2, db_from_node3]:
            node1.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")
