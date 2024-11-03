import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config.xml", "configs/config.d/backups.xml"],
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
    return instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")[0]


def restore_table(backup_name):
    return instance.query(f"RESTORE TABLE test.table FROM {backup_name}").split("\t")[0]


def test_backup_log():
    instance.query("SYSTEM FLUSH LOGS")
    instance.query("drop table system.backup_log")

    backup_name = "File('/backups/test_backup/')"
    assert instance.query("SELECT * FROM system.tables WHERE name = 'backup_log'") == ""

    backup_id = backup_table(backup_name)
    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(
        f"SELECT status, error FROM system.backup_log WHERE id='{backup_id}' ORDER BY event_date, event_time_microseconds"
    ) == TSV([["CREATING_BACKUP", ""], ["BACKUP_CREATED", ""]])

    instance.query("DROP TABLE test.table SYNC")

    restore_id = restore_table(backup_name)
    instance.query("SYSTEM FLUSH LOGS")
    assert instance.query(
        f"SELECT status, error FROM system.backup_log WHERE id='{restore_id}' ORDER BY event_date, event_time_microseconds"
    ) == TSV([["RESTORING", ""], ["RESTORED", ""]])

    instance.restart_clickhouse()

    assert instance.query(
        f"SELECT status, error FROM system.backup_log WHERE id='{backup_id}' ORDER BY event_date, event_time_microseconds"
    ) == TSV([["CREATING_BACKUP", ""], ["BACKUP_CREATED", ""]])
    assert instance.query(
        f"SELECT status, error FROM system.backup_log WHERE id='{restore_id}' ORDER BY event_date, event_time_microseconds"
    ) == TSV([["RESTORING", ""], ["RESTORED", ""]])
