import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)

INSERT_SETTINGS = {"allow_insert_into_iceberg": 1}


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_modify_column_basic(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Widen Int32 to Int64 (Iceberg int→long); existing and new rows read correctly."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_modify_column_basic_" + storage_type + "_" + get_uuid_str()

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

    instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN id Int64;", settings=INSERT_SETTINGS)

    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n"

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (3000000000, 'foo');", settings=INSERT_SETTINGS)
    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\thello\n2\tworld\n3000000000\tfoo\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_modify_column_errors(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Invalid schema evolution (e.g. String→Int64) must fail; columns unchanged."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_modify_column_errors_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN value Int64;",
        settings=INSERT_SETTINGS,
    )
    el = error.lower()
    # String→integer: mismatched Poco::Var kinds in checkValidSchemaEvolution → BadCastException
    assert (
        "bad cast" in el
        or "can not convert" in el
        or "cannot convert" in el
        or "schema evolution" in el
        or "doesn't allow" in el
    )

    assert instance.query(
        f"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '{TABLE_NAME}' ORDER BY name"
    ) == "id\nvalue\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_modify_column_noop_same_type(started_cluster_iceberg_no_spark, format_version, storage_type):
    """MODIFY COLUMN to the same type (Int32→Int32) is a no-op and must succeed silently."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_modify_noop_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'a');", settings=INSERT_SETTINGS)

    # MODIFY to the exact same type should be a silent no-op (no schema change).
    instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN id Int32;", settings=INSERT_SETTINGS)

    assert instance.query(f"SELECT id, value FROM {TABLE_NAME} ORDER BY id") == "1\ta\n"


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_modify_column_rejects_indistinguishable_type(started_cluster_iceberg_no_spark, format_version, storage_type):
    """MODIFY COLUMN id UInt32 on an Iceberg 'int' column (Int32) must fail because
    Iceberg represents both as 'int' and the change cannot be recorded."""
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_modify_reject_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(id Int32, value Nullable(String))",
        format_version,
    )

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, 'x');", settings=INSERT_SETTINGS)

    error = instance.query_and_get_error(
        f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN id UInt32;",
        settings=INSERT_SETTINGS,
    )
    assert "same iceberg type" in error.lower() or "cannot modify" in error.lower() or "bad_arguments" in error.lower()
