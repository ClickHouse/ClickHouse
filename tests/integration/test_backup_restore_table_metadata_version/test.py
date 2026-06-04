import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/backup_disk.xml"],
    with_zookeeper=True,
    macros={"shard": 0, "replica": 1},
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


def metadata_version(table):
    return node.query(
        f"SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = '{table}'"
    ).strip()


def create_table(table):
    node.query("CREATE DATABASE IF NOT EXISTS test_db")
    node.query(
        f"""
        CREATE TABLE test_db.{table} (id UInt64, name Nullable(String))
        ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/test_db/{table}', '{{replica}}')
        ORDER BY id
        """
    )


# Reproduces https://github.com/ClickHouse/ClickHouse/issues/67457
def test_restore_table_metadata_version(start_cluster):
    create_table("t1")
    node.query(
        "INSERT INTO test_db.t1 SELECT number, toString(number) FROM numbers(10)"
    )
    node.query(
        "ALTER TABLE test_db.t1 ADD COLUMN surname Nullable(String) AFTER name SETTINGS alter_sync = 2"
    )
    node.query(
        "INSERT INTO test_db.t1 SELECT number, toString(number), toString(number) FROM numbers(10, 10)"
    )
    assert metadata_version("t1") == "1"

    backup_name = new_backup_name()
    node.query(f"BACKUP TABLE test_db.t1 TO {backup_name}")
    node.query("DROP TABLE test_db.t1 SYNC")
    node.query(f"RESTORE TABLE test_db.t1 FROM {backup_name}")

    assert metadata_version("t1") == "1"

    # Merges of parts with metadata version 1 are not blocked (the symptom of #67457).
    node.query("OPTIMIZE TABLE test_db.t1 FINAL", timeout=120)
    assert (
        node.query(
            "SELECT count() FROM system.parts WHERE database = 'test_db' AND table = 't1' AND active"
        ).strip()
        == "1"
    )

    # ALTER works after the restore (the version in ZooKeeper matches the in-memory one).
    node.query(
        "ALTER TABLE test_db.t1 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
    )
    assert metadata_version("t1") == "2"

    # The restored metadata version survives a server restart.
    node.restart_clickhouse()
    assert_eq_with_retry(
        node,
        "SELECT metadata_version FROM system.tables WHERE database = 'test_db' AND name = 't1'",
        "2",
    )

    node.query("DROP TABLE test_db.t1 SYNC")


def test_restore_table_metadata_version_structure_only(start_cluster):
    create_table("t2")
    node.query(
        "ALTER TABLE test_db.t2 ADD COLUMN surname Nullable(String) AFTER name SETTINGS alter_sync = 2"
    )
    assert metadata_version("t2") == "1"

    backup_name = new_backup_name()
    node.query(
        f"BACKUP TABLE test_db.t2 TO {backup_name} SETTINGS structure_only = true"
    )
    node.query("DROP TABLE test_db.t2 SYNC")
    node.query(
        f"RESTORE TABLE test_db.t2 FROM {backup_name} SETTINGS structure_only = true"
    )

    assert metadata_version("t2") == "1"

    node.query(
        "ALTER TABLE test_db.t2 ADD COLUMN extra UInt8 SETTINGS alter_sync = 2"
    )
    assert metadata_version("t2") == "2"

    node.query("DROP TABLE test_db.t2 SYNC")
