import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/slow_backups.xml",
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


# Test that shutdown doesn't cancel a running backup and waits until it finishes.
def test_shutdown_wait_backup():
    node.query(
        "CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY x%5"
    )

    node.query(f"INSERT INTO tbl SELECT number FROM numbers(500)")

    backup_id = uuid.uuid4().hex
    start_backup(backup_id)

    node.restart_clickhouse()  # Must wait for the backup.

    # The information about this backup must be stored in system.backup_log
    assert node.query(
        f"SELECT status FROM system.backup_log WHERE id='{backup_id}' ORDER BY status"
    ) == TSV(["CREATING_BACKUP", "BACKUP_CREATED"])

    # The table can be restored from this backup.
    node.query("DROP TABLE tbl SYNC")
    node.query(f"RESTORE TABLE tbl FROM {get_backup_name(backup_id)}")
