import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_drop_column_basic(started_cluster_iceberg_no_spark, format_version, storage_type):
    """DROP COLUMN removes the column from reads and inserts; remaining columns unchanged."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_drop_column_basic_" + storage_type + "_" + get_uuid_str()

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

    instance.query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN value;", settings=INSERT_SETTINGS)

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == "1\n2\n"

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3);", settings=INSERT_SETTINGS)
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == "1\n2\n3\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_drop_column_errors(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Dropping a non-existent column must fail; table structure unchanged."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_drop_column_errors_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} DROP COLUMN nonexistent;",
        settings=INSERT_SETTINGS,
    )
    assert "nonexistent" in error

    assert instance.query(
        f"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{TABLE_NAME}' ORDER BY name"
    ) == "id\nvalue\n"
