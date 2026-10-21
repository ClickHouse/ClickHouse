"""RESTORE of an `Alias` table accepts a definition written by an older version, which inlined the
columns of the target table into it, and compares it equal to a definition written by the current one."""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# 26.9 stores the target's columns in an `Alias` definition, so both its metadata and its backups carry them.
node = cluster.add_instance(
    "node",
    main_configs=["configs/backups.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.9",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def new_backup(prefix):
    return f"File('/var/lib/clickhouse/backups/{prefix}_{uuid.uuid4().hex}')"


def restore_over_recreated_alias(table, backup):
    """`RESTORE` over a table which already exists compares its definition with the one in the backup."""
    node.query(f"DROP TABLE db.{table} SYNC")
    node.query(f"CREATE TABLE db.{table} ENGINE = Alias('t')")
    return node.query(f"RESTORE TABLE db.{table} FROM {backup}")


def test_restore_alias_table_created_by_older_version(start_cluster):
    node.query("CREATE DATABASE db")
    node.query(
        "CREATE TABLE db.t (id UInt64, value String) ENGINE = MergeTree ORDER BY id"
    )
    node.query("CREATE TABLE db.a ENGINE = Alias('t')")
    node.query("CREATE TABLE db.a_legacy ENGINE = Alias('t')")
    old_backup = new_backup("old")
    node.query(f"BACKUP TABLE db.a TO {old_backup}")
    assert "RESTORED" in restore_over_recreated_alias("a", old_backup)

    node.restart_with_latest_version()

    # The backup carries the columns, the alias it is restored over doesn't.
    assert "RESTORED" in restore_over_recreated_alias("a", old_backup)

    # The definition of an alias created before the upgrade still carries the columns, so a backup
    # taken after the upgrade must not carry them over to a restore of the alias recreated now.
    assert "UInt64" in node.query(
        "SELECT create_table_query FROM system.tables WHERE database = 'db' AND name = 'a_legacy'"
    )
    legacy_alias_backup = new_backup("new")
    node.query(f"BACKUP TABLE db.a_legacy TO {legacy_alias_backup}")
    assert "RESTORED" in restore_over_recreated_alias("a_legacy", legacy_alias_backup)
