import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_rename_column_basic(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Rename a column: existing rows are readable under the new name, and new inserts work."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_rename_column_basic_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'hello'), (2, 'world');", settings=INSERT_SETTINGS)
    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n"

    instance.query(f"ALTER TABLE {TABLE_NAME} RENAME COLUMN value TO label;", settings=INSERT_SETTINGS)

    # existing rows readable under the new name
    assert instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n"

    # new inserts work under the new name
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'foo');", settings=INSERT_SETTINGS)
    assert instance.query(f"SELECT id, label FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n3\tfoo\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_rename_column_errors(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Renaming a non-existent column or renaming to an already-used name must raise BAD_ARGUMENTS."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_rename_column_errors_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    # rename a column that does not exist — rejected by AlterCommands::validate (NOT_FOUND_COLUMN_IN_BLOCK)
    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} RENAME COLUMN nonexistent TO other;",
        settings=INSERT_SETTINGS,
    )
    assert "nonexistent" in error

    # rename to a name already used by another column — rejected by AlterCommands::validate (DUPLICATE_COLUMN)
    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} RENAME COLUMN value TO id;",
        settings=INSERT_SETTINGS,
    )
    assert "DUPLICATE_COLUMN" in error
    assert "id" in error

    # table structure must be unchanged after both failed renames
    assert instance.query(f"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{TABLE_NAME}' ORDER BY name") == "id\nvalue\n"
