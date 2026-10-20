import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


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
        node.query(f"INSERT INTO {table} SELECT [['old']]")
        add_legacy_text_index(table, "Array(Array(String))")

        assert node.query(
            f"SELECT count() FROM system.data_skipping_indices WHERE database = 'default' AND table = '{table}' AND name = 'idx'"
        ) == "1\n"
        assert "NOT_IMPLEMENTED" in node.query_and_get_error(f"INSERT INTO {table} SELECT [['new']]")

        # An unrelated ALTER revalidates every index of the table, including the inherited legacy one.
        node.query(f"ALTER TABLE {table} ADD COLUMN n UInt8 DEFAULT 0")

        # Replacing the index is a new declaration, even when it reuses the same name.
        assert "BAD_ARGUMENTS" in node.query_and_get_error(
            f"ALTER TABLE {table} DROP INDEX idx, ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')"
        )

        node.query(f"ALTER TABLE {table} DROP INDEX idx")
        node.query(f"INSERT INTO {table} (t) SELECT [['new']]")
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

        # Changing the indexed column's type is not an inherited definition: the new base type
        # is rejected even though the old multidimensional `text` index also fails validation.
        assert "BAD_ARGUMENTS" in node.query_and_get_error(
            f"ALTER TABLE {table} MODIFY COLUMN t Array(Array(UInt8))"
        )

        node.query(f"ALTER TABLE {table} ADD COLUMN n UInt8 DEFAULT 0")
        node.query(f"INSERT INTO {table} SELECT [['abcdefgh', 'ijklmnop']], 1")
        node.query(f"DETACH TABLE {table} SYNC")
        node.query(f"ATTACH TABLE {table}")
        node.query(f"INSERT INTO {table} SELECT [['qrstuvwx', 'yzabcdef']], 2")
        assert node.query(f"SELECT count() FROM {table}") == "2\n"
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
