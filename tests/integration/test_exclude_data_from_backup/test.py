import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/backups_disk.xml"],
    external_dirs=["/backups/"],
)

backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}/')"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_exclude_data_from_backup():
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t")
    instance.query(
        "CREATE TABLE test.t (id UInt64) ENGINE = MergeTree ORDER BY id "
        "SETTINGS exclude_data_from_backup = 1"
    )
    instance.query("INSERT INTO test.t VALUES (1), (2), (3)")
    assert instance.query("SELECT count() FROM test.t") == "3\n"

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.t TO {backup_name}")

    instance.query("DROP TABLE test.t")
    instance.query(f"RESTORE TABLE test.t FROM {backup_name}")

    # Data should NOT be restored (it was excluded), but table/schema should exist.
    assert instance.query("SELECT count() FROM test.t") == "0\n"
    assert instance.query(
        "SELECT name, type FROM system.columns WHERE database='test' AND table='t'"
    ) == "id\tUInt64\n"

    instance.query("DROP TABLE IF EXISTS test.t")


def test_exclude_data_from_backup_default_false():
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.t2")
    instance.query(
        "CREATE TABLE test.t2 (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query("INSERT INTO test.t2 VALUES (1), (2), (3)")

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.t2 TO {backup_name}")

    instance.query("DROP TABLE test.t2")
    instance.query(f"RESTORE TABLE test.t2 FROM {backup_name}")

    # Default setting is false, so data SHOULD be restored normally.
    assert instance.query("SELECT count() FROM test.t2") == "3\n"

    instance.query("DROP TABLE IF EXISTS test.t2")


def test_exclude_data_from_backup_materialized_view_inner_table():
    instance.query("CREATE DATABASE IF NOT EXISTS test")
    instance.query("DROP TABLE IF EXISTS test.mv")
    instance.query("DROP TABLE IF EXISTS test.mv_source")
    instance.query(
        "CREATE TABLE test.mv_source (id UInt64) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(
        "CREATE MATERIALIZED VIEW test.mv ENGINE = MergeTree ORDER BY id "
        "AS SELECT id FROM test.mv_source"
    )
    instance.query("INSERT INTO test.mv_source VALUES (1), (2), (3)")
    assert instance.query("SELECT count() FROM test.mv") == "3\n"

    inner_uuid = instance.query(
        "SELECT uuid FROM system.tables WHERE database='test' AND name='mv'"
    ).strip()

    instance.query(
        f"ALTER TABLE test.`.inner_id.{inner_uuid}` "
        "MODIFY SETTING exclude_data_from_backup = 1"
    )

    backup_name = new_backup_name()
    instance.query(f"BACKUP TABLE test.mv TO {backup_name}")

    instance.query("DROP TABLE test.mv")
    instance.query(f"RESTORE TABLE test.mv FROM {backup_name}")

    # Data should NOT be restored (inner table was excluded), but the view
    # definition and its target table schema should still exist.
    assert instance.query("SELECT count() FROM test.mv") == "0\n"

    instance.query("DROP TABLE IF EXISTS test.mv")
