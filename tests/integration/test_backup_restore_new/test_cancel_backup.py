import re
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

# We expect backup/restore query cancellation to be fast enough,
# though in CI cancellation may occasionally take slightly more
# then 2 seconds (2000 - 2500 ms).
kill_duration_ms_threshold = 4000

# main_configs are copied into config.d/ of the instance.
shutdown_wait_config = "/etc/clickhouse-server/config.d/shutdown_wait_unfinished.xml"
shutdown_wait_seconds = 10

cluster = ClickHouseCluster(__file__)

main_configs = [
    "configs/backups_disk.xml",
    "configs/slow_backups.xml",
    "configs/shutdown_cancel_backups.xml",
    "configs/shutdown_wait_unfinished.xml",
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

    backup_duration = float(
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
    assert kill_duration_ms < kill_duration_ms_threshold


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

    restore_duration = float(
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
    assert kill_duration_ms < kill_duration_ms_threshold


# Test that BACKUP and RESTORE operations can be cancelled with KILL QUERY.
def test_cancel_backup():
    # We use partitioning so backups would contain more files.
    node.query(
        "CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY x%20"
    )

    node.query("INSERT INTO tbl SELECT number FROM numbers(500)")

    try_backup_id_1 = uuid.uuid4().hex
    start_backup(try_backup_id_1)
    cancel_backup(try_backup_id_1)

    backup_id = uuid.uuid4().hex
    start_backup(backup_id)
    wait_backup(backup_id)

    node.query("DROP TABLE tbl SYNC")

    try_restore_id_1 = uuid.uuid4().hex
    start_restore(try_restore_id_1, backup_id)
    cancel_restore(try_restore_id_1)

    # IF EXISTS because it's unknown whether RESTORE had managed to create a table before it got cancelled.
    node.query("DROP TABLE IF EXISTS tbl SYNC")

    restore_id = uuid.uuid4().hex
    start_restore(restore_id, backup_id)
    wait_restore(restore_id)


# Test that shutdown cancels a running backup and doesn't wait until it finishes.
def test_shutdown_cancel_backup():
    node.query(
        "CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple() PARTITION BY x%5"
    )

    node.query("INSERT INTO tbl SELECT number FROM numbers(500)")

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


# Test that a backup which cannot observe its own cancellation does not keep the server from
# terminating on a single SIGTERM.
def test_shutdown_cancel_wedged_backup():
    node.query("CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple()")
    node.query("INSERT INTO tbl SELECT number FROM numbers(100)")

    backup_id = uuid.uuid4().hex
    try:
        # This parks the backup at the top of doBackup, before any checkTimeLimit() check,
        # so the operation never observes a cancellation request.
        node.query("SYSTEM ENABLE FAILPOINT backup_pause_on_start")
        node.query(
            f"BACKUP TABLE tbl TO {get_backup_name(backup_id)}"
            f" SETTINGS id='{backup_id}' ASYNC"
        )
        node.query("SYSTEM WAIT FAILPOINT backup_pause_on_start PAUSE")

        assert (
            node.query(f"SELECT status FROM system.backups WHERE id='{backup_id}'")
            == "CREATING_BACKUP\n"
        )

        # True means the process exited on its own; None means this had to escalate to SIGKILL.
        assert node.stop_clickhouse(stop_wait_sec=30, kill=False) is True
    finally:
        node.start_clickhouse()
        node.query("SYSTEM DISABLE FAILPOINT backup_pause_on_start")
        assert (
            node.query("SELECT count() FROM system.fail_points WHERE enabled") == "0\n"
        )


# Test that the wait above is the one configured by shutdown_wait_unfinished rather than any
# fixed duration: with a large value the server must keep waiting past the SIGTERM window.
# Replacing the configured deadline with a constant leaves the test above passing and makes
# this one fail.
def test_shutdown_wait_for_backup_is_configurable():
    node.replace_in_config(
        shutdown_wait_config,
        f"<shutdown_wait_unfinished>{shutdown_wait_seconds}</shutdown_wait_unfinished>",
        "<shutdown_wait_unfinished>600</shutdown_wait_unfinished>",
    )
    try:
        # The setting is not changeable without a restart, so SYSTEM RELOAD CONFIG would not
        # apply it.
        node.restart_clickhouse()
        assert (
            node.query(
                "SELECT value FROM system.server_settings"
                " WHERE name = 'shutdown_wait_unfinished'"
            )
            == "600\n"
        )

        node.query("CREATE TABLE tbl (x UInt64) ENGINE=MergeTree() ORDER BY tuple()")
        node.query("INSERT INTO tbl SELECT number FROM numbers(100)")

        backup_id = uuid.uuid4().hex
        node.query("SYSTEM ENABLE FAILPOINT backup_pause_on_start")
        node.query(
            f"BACKUP TABLE tbl TO {get_backup_name(backup_id)}"
            f" SETTINGS id='{backup_id}' ASYNC"
        )
        node.query("SYSTEM WAIT FAILPOINT backup_pause_on_start PAUSE")

        assert (
            node.query(f"SELECT status FROM system.backups WHERE id='{backup_id}'")
            == "CREATING_BACKUP\n"
        )

        # None means the stop had to escalate to SIGKILL, i.e. the server was still honouring
        # the 600 second wait. 600 is also outside the 180 second window stop_clickhouse uses
        # on a coverage build, so this holds for every build type.
        assert node.stop_clickhouse(stop_wait_sec=30, kill=False) is None
    finally:
        # The server is down after the forced kill, so it picks the restored value up on start.
        node.replace_in_config(
            shutdown_wait_config,
            "<shutdown_wait_unfinished>600</shutdown_wait_unfinished>",
            f"<shutdown_wait_unfinished>{shutdown_wait_seconds}</shutdown_wait_unfinished>",
        )
        node.start_clickhouse()
        node.query("SYSTEM DISABLE FAILPOINT backup_pause_on_start")
        assert (
            node.query("SELECT count() FROM system.fail_points WHERE enabled") == "0\n"
        )
