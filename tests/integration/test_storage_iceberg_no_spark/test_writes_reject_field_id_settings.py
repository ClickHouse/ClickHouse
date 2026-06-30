import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_reject_field_id_settings(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """Regression coverage for the ParquetBlockOutputFormat datalake guard.

    When writing to an Iceberg table the datalake metadata (the `column_mapper`)
    is the authoritative source for Parquet `field_id`s. Letting the session
    settings `output_format_parquet_column_field_ids` /
    `output_format_parquet_auto_assign_field_ids` override that mapping would
    emit `field_id`s that no longer match the table metadata, breaking
    subsequent reads, so `ParquetBlockOutputFormat` rejects them with
    `BAD_ARGUMENTS`.

    This path is not reachable from a stateless (`file()` / `clickhouse-local`)
    test because no `column_mapper` is present there; without this test,
    removing the guard would leave the stateless test green.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = (
        "test_writes_reject_field_id_settings_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
    )

    # A plain INSERT works — the datalake column-id mapping is used.
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1), (2);")
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "1\n2\n"

    # Auto-assigning field ids on top of a datalake mapping is rejected.
    error = instance.query_and_get_error(
        f"INSERT INTO {TABLE_NAME} VALUES (3);",
        settings={"output_format_parquet_auto_assign_field_ids": 1},
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error

    # An explicit field-id override map on top of a datalake mapping is rejected too.
    error = instance.query_and_get_error(
        f"INSERT INTO {TABLE_NAME} VALUES (4);",
        settings={"output_format_parquet_column_field_ids": "{'x': '1'}"},
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error

    # The rejected inserts must not have committed any rows or corrupted the table.
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == "1\n2\n"
