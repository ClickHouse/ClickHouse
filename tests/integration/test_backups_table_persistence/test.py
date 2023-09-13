import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV


cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config.d/backups.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def backup_table(backup_name):
    instance.query("CREATE DATABASE test")
    instance.query("CREATE TABLE test.table(x UInt32) ENGINE=MergeTree ORDER BY x")
    instance.query("INSERT INTO test.table SELECT number FROM numbers(10)")
    return instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")


def restore_table(backup_name):
    return instance.query(f"RESTORE TABLE test.table FROM {backup_name}").split("\t")


def test_system_backups():
    backup_name = "File('/backups/test_backup/')"
    assert instance.query("SELECT * FROM system.backups") == ""

    [backup_id, status] = backup_table(backup_name)
    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"
    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{backup_id}'",
        TSV([["BACKUP_CREATED", ""]]),
        200,
    )

    instance.query("DROP TABLE test.table SYNC")

    [restore_id, status] = restore_table(backup_name)
    assert status == "RESTORING\n" or status == "RESTORED\n"
    assert_eq_with_retry(
        instance,
        f"SELECT status, error FROM system.backups WHERE id='{restore_id}'",
        TSV([["RESTORED", ""]]),
        200,
    )

    instance.restart_clickhouse()

    assert instance.query(
        f"SELECT status, error FROM system.backups WHERE id='{backup_id}'"
    ) == TSV([["BACKUP_CREATED", ""]])
    assert instance.query(
        f"SELECT status, error FROM system.backups WHERE id='{restore_id}'"
    ) == TSV([["RESTORED", ""]])
