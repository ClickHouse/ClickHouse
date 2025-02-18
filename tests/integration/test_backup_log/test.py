import pytest
import uuid

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
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("CREATE TABLE IF NOT EXISTS test.table(x UInt32) ENGINE=MergeTree ORDER BY x")
    instance.query("INSERT INTO test.table SELECT number FROM numbers(10)")
    return instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")[0]


def restore_table(backup_name):
    return instance.query(f"RESTORE TABLE test.table FROM {backup_name}").split("\t")[0]


def test_backup_log():
    def get_system_backup_log_records(backup_id: int):
        return instance.query(
            f"""
                SELECT
                    status,
                    if(status = 'BACKUP_CREATED', throwIf(start_time > end_time), 0),
                    if(status = 'CREATING_BACKUP', throwIf(end_time != '1970-01-01 00:00:00.000000'), 0),
                    throwIf(start_time == '1970-01-01 00:00:00.000000'),
                    error
                FROM system.backup_log WHERE id='{backup_id}'
                ORDER BY event_date, event_time_microseconds
            """
        )

    def get_system_restore_logs_records(restore_id: int):
        return instance.query(
            f"""
                SELECT
                    status,
                    if(status = 'RESTORED', throwIf(start_time > end_time), 0),
                    if(status = 'RESTORING', throwIf(end_time != '1970-01-01 00:00:00.000000'), 0),
                    throwIf(start_time == '1970-01-01 00:00:00.000000'),
                    error
                FROM system.backup_log
                WHERE id='{restore_id}'
                ORDER BY event_date, event_time_microseconds"""
        )

    instance.query("SYSTEM FLUSH LOGS")
    instance.query("DROP TABLE IF EXISTS system.backup_log SYNC")

    backup_id = uuid.uuid4().hex
    backup_name = f"File('/backups/test_backup_{backup_id}/')"
    assert instance.query("SELECT * FROM system.tables WHERE name = 'backup_log'") == ""

    backup_id = backup_table(backup_name)
    instance.query("SYSTEM FLUSH LOGS")
    assert get_system_backup_log_records(backup_id) == TSV(
        [["CREATING_BACKUP", 0, 0, 0, ""], ["BACKUP_CREATED", 0, 0, 0, ""]]
    )

    instance.query("DROP TABLE IF EXISTS test.table SYNC")

    restore_id = restore_table(backup_name)
    instance.query("SYSTEM FLUSH LOGS")
    assert get_system_restore_logs_records(restore_id) == TSV(
        [["RESTORING", 0, 0, 0, ""], ["RESTORED", 0, 0, 0, ""]]
    )

    instance.restart_clickhouse()

    assert get_system_backup_log_records(backup_id) == TSV(
        [["CREATING_BACKUP", 0, 0, 0, ""], ["BACKUP_CREATED", 0, 0, 0, ""]]
    )
    assert get_system_restore_logs_records(restore_id) == TSV(
        [["RESTORING", 0, 0, 0, ""], ["RESTORED", 0, 0, 0, ""]]
    )
