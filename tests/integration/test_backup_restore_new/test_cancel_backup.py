import re
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/slow_backups.xml",
    "configs/shutdown_cancel_backups.xml",
]

node = cluster.add_instance(
    "node",
    main_configs=main_configs,
    external_dirs=["/backups/"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS tbl SYNC")


# Generate the backup name.
def get_backup_name(backup_id):
    return f"Disk('backups', '{backup_id}')"


# Start making a backup asynchronously.
def start_backup(backup_id):
    node.query(
        f"BACKUP TABLE tbl TO {get_backup_name(backup_id)} SETTINGS id='{backup_id}' ASYNC"
    )

    assert (
        node.query(f"SELECT status FROM system.backups WHERE id='{backup_id}'")
        == "CREATING_BACKUP\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.processes WHERE query_kind='Backup' AND query LIKE '%{backup_id}%'"
        )
        == "1\n"
    )


# Wait for the backup to be completed.
def wait_backup(backup_id):
    assert_eq_with_retry(
        node,
        f"SELECT status FROM system.backups WHERE id='{backup_id}'",
        "BACKUP_CREATED",
        retry_count=60,
        sleep_time=5,
    )

    backup_duration = int(
        node.query(
            f"SELECT end_time - start_time FROM system.backups WHERE id='{backup_id}'"
        )
    )
    assert backup_duration >= 3  # Backup is not expected to be too quick in this test.


# Cancel the specified backup.
def cancel_backup(backup_id):
    node.query(
        f"KILL QUERY WHERE query_kind='Backup' AND query LIKE '%{backup_id}%' SYNC"
    )
    assert (
        node.query(f"SELECT status FROM system.backups WHERE id='{backup_id}'")
        == "BACKUP_CANCELLED\n"
    )
    expected_error = "QUERY_WAS_CANCELLED"
    assert expected_error in node.query(
        f"SELECT error FROM system.backups WHERE id='{backup_id}'"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.processes WHERE query_kind='Backup' AND query LIKE '%{backup_id}%'"
        )
        == "0\n"
    )
    node.query("SYSTEM FLUSH LOGS")
    kill_duration_ms = int(
        node.query(
            f"SELECT query_duration_ms FROM system.query_log WHERE query_kind='KillQuery' AND query LIKE '%{backup_id}%' AND type='QueryFinish'"
        )
    )
    assert kill_duration_ms < 2000  # Query must be cancelled quickly


# Start restoring from a backup.
def start_restore(restore_id, backup_id):
    node.query(
        f"RESTORE TABLE tbl FROM {get_backup_name(backup_id)} SETTINGS id='{restore_id}' ASYNC"
    )

    assert (
        node.query(f"SELECT status FROM system.backups WHERE id='{restore_id}'")
        == "RESTORING\n"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.processes WHERE query_kind='Restore' AND query LIKE '%{restore_id}%'"
        )
        == "1\n"
    )


# Wait for the restore operation to be completed.
def wait_restore(restore_id):
    assert_eq_with_retry(
        node,
        f"SELECT status FROM system.backups WHERE id='{restore_id}'",
        "RESTORED",
        retry_count=60,
        sleep_time=5,
    )

    restore_duration = int(
        node.query(
            f"SELECT end_time - start_time FROM system.backups WHERE id='{restore_id}'"
        )
    )
    assert (
        restore_duration >= 3
    )  # Restore is not expected to be too quick in this test.


# Cancel the specified restore operation.
def cancel_restore(restore_id):
    node.query(
        f"KILL QUERY WHERE query_kind='Restore' AND query LIKE '%{restore_id}%' SYNC"
    )
    assert (
        node.query(f"SELECT status FROM system.backups WHERE id='{restore_id}'")
        == "RESTORE_CANCELLED\n"
    )
    expected_error = "QUERY_WAS_CANCELLED"
    assert expected_error in node.query(
        f"SELECT error FROM system.backups WHERE id='{restore_id}'"
    )
    assert (
        node.query(
            f"SELECT count() FROM system.processes WHERE query_kind='Restore' AND query LIKE '%{restore_id}%'"
        )
        == "0\n"
    )
    node.query("SYSTEM FLUSH LOGS")
    kill_duration_ms = int(
        node.query(
            f"SELECT query_duration_ms FROM system.query_log WHERE query_kind='KillQuery' AND query LIKE '%{restore_id}%' AND type='QueryFinish'"
        )
    )
    assert kill_duration_ms < 2000  # Query must be cancelled quickly


# Test that BACKUP and RESTORE operations can be cancelled with KILL QUERY.
def test_cancel_backup():
    # We use partitioning so backups would contain more files.
    node.query(
        "CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY x%20"
    )

    node.query(f"INSERT INTO tbl SELECT number FROM numbers(500)")

    try_backup_id_1 = uuid.uuid4().hex
    start_backup(try_backup_id_1)
    cancel_backup(try_backup_id_1)

    backup_id = uuid.uuid4().hex
    start_backup(backup_id)
    wait_backup(backup_id)

    node.query(f"DROP TABLE tbl SYNC")

    try_restore_id_1 = uuid.uuid4().hex
    start_restore(try_restore_id_1, backup_id)
    cancel_restore(try_restore_id_1)

    # IF EXISTS because it's unknown whether RESTORE had managed to create a table before it got cancelled.
    node.query(f"DROP TABLE IF EXISTS tbl SYNC")

    restore_id = uuid.uuid4().hex
    start_restore(restore_id, backup_id)
    wait_restore(restore_id)


# Test that shutdown cancels a running backup and doesn't wait until it finishes.
def test_shutdown_cancel_backup():
    node.query(
        "CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY x%5"
    )

    node.query(f"INSERT INTO tbl SELECT number FROM numbers(500)")

    backup_id = uuid.uuid4().hex
    start_backup(backup_id)

    node.restart_clickhouse()  # Must cancel the backup.

    # The information about this cancelled backup must be stored in system.backup_log
    assert node.query(
        f"SELECT status FROM system.backup_log WHERE id='{backup_id}' ORDER BY status"
    ) == TSV(["CREATING_BACKUP", "BACKUP_CANCELLED"])

    # The table can't be restored from this backup.
    expected_error = "Backup .* not found"
    node.query("DROP TABLE tbl SYNC")
    assert re.search(
        expected_error,
        node.query_and_get_error(
            f"RESTORE TABLE tbl FROM {get_backup_name(backup_id)}"
        ),
    )
