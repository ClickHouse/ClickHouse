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
    """Regression coverage for the `ParquetBlockOutputFormat` datalake guard.

    When writing to an Iceberg table the datalake metadata (the `column_mapper`)
    is the authoritative source for Parquet `field_id`s. Letting the settings
    `output_format_parquet_column_field_ids` /
    `output_format_parquet_auto_assign_field_ids` override that mapping would
    emit `field_id`s that no longer match the table metadata, breaking
    subsequent reads, so `ParquetBlockOutputFormat` rejects them with
    `BAD_ARGUMENTS`.

    This path is not reachable from a stateless (`file()` / `clickhouse-local`)
    test because no `column_mapper` is present there.

    The field-id settings must be baked into the table's `SETTINGS` clause at
    `CREATE TABLE` time: an object-storage engine freezes its `FormatSettings`
    from the `SETTINGS` clause and ignores per-`INSERT` session settings, so a
    per-query field-id setting would never reach the writer and the guard would
    not fire. The settings parse and freeze at `CREATE` time; the guard rejects
    them only at write time, when the datalake `column_mapper` is present.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    def make_table_name(suffix):
        return (
            "test_writes_reject_field_id_settings_"
            + storage_type
            + "_"
            + suffix
            + "_"
            + get_uuid_str()
        )

    # A plain INSERT works — the datalake column-id mapping is used.
    ok_table = make_table_name("ok")
    create_iceberg_table(
        storage_type,
        instance,
        ok_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
    )
    instance.query(f"INSERT INTO {ok_table} VALUES (1), (2);")
    assert instance.query(f"SELECT * FROM {ok_table} ORDER BY ALL") == "1\n2\n"

    # Auto-assigning field ids on top of a datalake mapping is rejected. The
    # setting is frozen into the table at CREATE time (see the docstring).
    auto_table = make_table_name("auto")
    create_iceberg_table(
        storage_type,
        instance,
        auto_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
        additional_settings=["output_format_parquet_auto_assign_field_ids = 1"],
    )
    error = instance.query_and_get_error(f"INSERT INTO {auto_table} VALUES (3);")
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error

    # An explicit field-id override map on top of a datalake mapping is rejected too.
    explicit_table = make_table_name("explicit")
    create_iceberg_table(
        storage_type,
        instance,
        explicit_table,
        started_cluster_iceberg_no_spark,
        "(x Int32)",
        format_version,
        additional_settings=["output_format_parquet_column_field_ids = {'x': '1'}"],
    )
    error = instance.query_and_get_error(f"INSERT INTO {explicit_table} VALUES (4);")
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error

    # The rejected inserts must not have committed any rows or corrupted the table.
    assert instance.query(f"SELECT * FROM {auto_table} ORDER BY ALL") == ""
    assert instance.query(f"SELECT * FROM {explicit_table} ORDER BY ALL") == ""
