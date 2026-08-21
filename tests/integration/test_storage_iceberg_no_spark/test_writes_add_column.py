import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_add_column_basic(started_cluster_iceberg_no_spark, format_version, storage_type):
    """ADD COLUMN (nullable): existing rows read with NULL in the new column; new inserts can set it."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_add_column_basic_" + storage_type + "_" + get_uuid_str()

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

    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN extra Nullable(Int32);", settings=INSERT_SETTINGS)

    assert instance.query(f"SELECT id, value, extra FROM {TABLE_NAME} ORDER BY id") == (
        "1\thello\t\\N\n2\tworld\t\\N\n"
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3, 'foo', 7);", settings=INSERT_SETTINGS)
    assert instance.query(f"SELECT id, value, extra FROM {TABLE_NAME} ORDER BY id") == (
        "1\thello\t\\N\n2\tworld\t\\N\n3\tfoo\t7\n"
    )


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_add_column_errors(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Non-nullable ADD COLUMN and duplicate name must fail; schema unchanged."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_add_column_errors_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN bad Int32;",
        settings=INSERT_SETTINGS,
    )
    assert "non-nullable" in error.lower() or "doesn't allow" in error.lower()

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} ADD COLUMN value Nullable(Int32);",
        settings=INSERT_SETTINGS,
    )
    assert "DUPLICATE_COLUMN" in error or "already exists" in error

    assert instance.query(
        f"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{TABLE_NAME}' ORDER BY name"
    ) == "id\nvalue\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_add_column_bool_and_decimal(started_cluster_iceberg_no_spark, format_version, storage_type):
    """ADD COLUMN with Bool (Iceberg boolean) and Decimal (Iceberg decimal) types."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_add_column_bool_dec_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a'), (2, 'b');", settings=INSERT_SETTINGS)

    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN flag Nullable(Bool);", settings=INSERT_SETTINGS)
    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN price Nullable(Decimal(10, 2));", settings=INSERT_SETTINGS)

    assert instance.query(f"SELECT id, value, flag, price FROM {TABLE_NAME} ORDER BY id") == (
        "1\ta\t\\N\t\\N\n2\tb\t\\N\t\\N\n"
    )

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3, 'c', true, 99.95), (4, 'd', false, 123.40);",
        settings=INSERT_SETTINGS,
    )
    assert instance.query(f"SELECT id, flag, price FROM {TABLE_NAME} ORDER BY id") == (
        "1\t\\N\t\\N\n2\t\\N\t\\N\n3\ttrue\t99.95\n4\tfalse\t123.4\n"
    )
