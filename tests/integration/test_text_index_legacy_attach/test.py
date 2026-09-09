import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
old_node = cluster.add_instance(
    "old_node",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag="26.8",
    with_installed_binary=True,
    macros={"replica": "old", "shard": "shard1"},
)
new_node = cluster.add_instance(
    "new_node",
    with_zookeeper=True,
    stay_alive=True,
    macros={"replica": "new", "shard": "shard1"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def add_legacy_text_index(table, column_type):
    node.query(f"DETACH TABLE {table} SYNC")
    metadata_path = node.query(
        f"SELECT metadata_path FROM system.detached_tables WHERE database = 'default' AND table = '{table}'"
    ).strip()
    replace_text_in_metadata(
        node,
        metadata_path,
        f"`t` {column_type}",
        f"`t` {column_type},\n    INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')",
    )

    database_disk_name = get_database_disk_name(node)
    if database_disk_name != "default":
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {database_disk_name}")

    node.query(f"ATTACH TABLE {table}")


def test_legacy_nested_string_index_attaches(started_cluster):
    table = "legacy_nested_string"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        node.query(f"CREATE TABLE {table} (t Array(Array(String))) ENGINE = MergeTree ORDER BY tuple()")
        node.query(f"INSERT INTO {table} VALUES ([['old']])")
        add_legacy_text_index(table, "Array(Array(String))")

        assert node.query(
            f"SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = '{table}' AND name = 'idx'"
        ) == "1\n"
        assert "NOT_IMPLEMENTED" in node.query_and_get_error(f"INSERT INTO {table} VALUES ([['new']])")

        node.query(f"ALTER TABLE {table} DROP INDEX idx")
        node.query(f"INSERT INTO {table} VALUES ([['new']])")
        assert node.query(f"SELECT count() FROM {table}") == "2\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_legacy_nested_fixed_string_index_attaches(started_cluster):
    table = "legacy_nested_fixed_string"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        node.query(f"CREATE TABLE {table} (t Array(Array(FixedString(8)))) ENGINE = MergeTree ORDER BY tuple()")
        add_legacy_text_index(table, "Array(Array(FixedString(8)))")

        assert node.query(
            f"SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = '{table}' AND name = 'idx'"
        ) == "1\n"
        node.query(f"INSERT INTO {table} VALUES ([['abcdefgh', 'ijklmnop']])")
        node.query(f"DETACH TABLE {table} SYNC")
        node.query(f"ATTACH TABLE {table}")
        node.query(f"INSERT INTO {table} VALUES ([['qrstuvwx', 'yzabcdef']])")
        assert node.query(f"SELECT count() FROM {table}") == "2\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_legacy_text_index_alter_replays_on_new_replica(started_cluster):
    database = "legacy_text_index_alter_replay"
    table = "t"
    created_table = "created"
    attached_table = "attached"
    database_path = f"/clickhouse/databases/{database}"

    for instance in (old_node, new_node):
        instance.query(f"DROP DATABASE IF EXISTS {database} SYNC")

    try:
        old_node.query(
            f"CREATE DATABASE {database} ENGINE = Replicated('{database_path}', 'shard1', 'old')"
        )
        new_node.query(
            f"CREATE DATABASE {database} ENGINE = Replicated('{database_path}', 'shard1', 'new')"
        )
        old_node.query(
            f"CREATE TABLE {database}.{table} (t Array(Array(String))) "
            "ENGINE = ReplicatedMergeTree ORDER BY tuple()"
        )
        new_node.query(f"SYSTEM SYNC DATABASE REPLICA {database}")
        new_node.query(f"SYSTEM SYNC REPLICA {database}.{table}")

        new_node.stop_clickhouse()
        try:
            old_node.query(
                f"ALTER TABLE {database}.{table} "
                "ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')",
                settings={"distributed_ddl_task_timeout": 0},
            )
            old_node.query(
                f"CREATE TABLE {database}.{created_table} "
                "(t Array(Array(String)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) "
                "ENGINE = MergeTree ORDER BY tuple()",
                settings={"distributed_ddl_task_timeout": 0},
            )
            attached_table_uuid = old_node.query("SELECT generateUUIDv4()").strip()
            old_node.query(
                f"ATTACH TABLE {database}.{attached_table} UUID '{attached_table_uuid}' "
                "(t Array(Array(String)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) "
                "ENGINE = ReplicatedMergeTree ORDER BY tuple()",
                settings={"distributed_ddl_task_timeout": 0},
            )
        finally:
            new_node.start_clickhouse()

        new_node.query(f"SYSTEM SYNC DATABASE REPLICA {database}", timeout=60)

        # Database replication creates the table, while the `ALTER` metadata entry belongs to
        # the ReplicatedMergeTree queue and must be pulled and executed separately.
        new_node.query(f"SYSTEM SYNC REPLICA {database}.{table} PULL", timeout=60)

        for table_name in (table, created_table, attached_table):
            assert_eq_with_retry(
                new_node,
                "SELECT count() FROM system.data_skipping_indices "
                f"WHERE database = '{database}' AND table = '{table_name}' AND name = 'idx'",
                "1",
                retry_count=120,
            )

        assert "INDEX idx t TYPE text" in new_node.query(
            f"SHOW CREATE TABLE {database}.{table}"
        )
        assert "INDEX idx t TYPE text" in new_node.query(
            f"SHOW CREATE TABLE {database}.{attached_table}"
        )
    finally:
        for instance in (old_node, new_node):
            instance.query(f"DROP DATABASE IF EXISTS {database} SYNC")
