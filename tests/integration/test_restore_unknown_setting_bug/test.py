"""
Proof test for bug: RestorerFromBackup::createTable uses non-existent setting names
causing RESTORE to fail with UNKNOWN_SETTING error.

The bug is at src/Backups/RestorerFromBackup.cpp:779-780:
  create_query_context->setSetting("keeper_initial_backoff_ms", ...);  // WRONG
  create_query_context->setSetting("keeper_max_backoff_ms", ...);      // WRONG

Correct setting names are:
  keeper_retry_initial_backoff_ms
  keeper_retry_max_backoff_ms

This test creates a ReplicatedMergeTree table, backs it up, drops it, and
restores it. On buggy code, the RESTORE fails with UNKNOWN_SETTING error.
After the fix, RESTORE succeeds.
"""

import uuid
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    main_configs=["configs/backups_disk.xml"],
    user_configs=["configs/zookeeper_retries.xml"],
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Ensure cleanup happens even if test fails."""
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS test_tbl SYNC")


def test_restore_replicated_table_uses_correct_keeper_settings(started_cluster):
    """
    This test exposes PR #72682 bug: wrong setting names in RestorerFromBackup.

    On buggy code: RESTORE fails with "Unknown setting 'keeper_initial_backoff_ms'"
    After fix: RESTORE succeeds.
    """
    # Cleanup from any previous runs
    node.query("DROP TABLE IF EXISTS test_tbl SYNC")

    # Create ReplicatedMergeTree table with data
    node.query(
        "CREATE TABLE test_tbl (x UInt32, y String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_restore_setting_bug_tbl', '{replica}') "
        "ORDER BY x"
    )
    node.query("INSERT INTO test_tbl VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    # Backup the table - use UUID to ensure unique backup name per run
    backup_id = uuid.uuid4().hex
    backup_name = f"Disk('backups', 'test_restore_setting_bug_{backup_id}/')"
    node.query(f"BACKUP TABLE test_tbl TO {backup_name}")

    # Drop the table
    node.query("DROP TABLE test_tbl SYNC")

    # RESTORE the table - this fails on buggy code with:
    # Code: 115. DB::Exception: Unknown setting 'keeper_initial_backoff_ms'
    # After fix, this should succeed
    node.query(f"RESTORE TABLE test_tbl FROM {backup_name}")

    # Verify data is restored correctly
    result = node.query("SELECT * FROM test_tbl ORDER BY x")
    assert result.strip() == "1\ta\n2\tb\n3\tc"
