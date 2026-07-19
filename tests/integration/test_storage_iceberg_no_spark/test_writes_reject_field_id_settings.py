import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_reject_field_id_settings(
    started_cluster_iceberg_no_spark, format_version, storage_type
):
    """Regression coverage for rejecting user Parquet field-id settings on Iceberg tables.

    When writing to an Iceberg table the datalake metadata (the `column_mapper`)
    is the authoritative source for Parquet `field_id`s. Letting the settings
    `output_format_parquet_column_field_ids` /
    `output_format_parquet_auto_assign_field_ids` override that mapping would
    emit `field_id`s that no longer match the table metadata, breaking
    subsequent reads.

    An object-storage engine freezes its `FormatSettings` from the `CREATE
    TABLE ... SETTINGS` clause and ignores per-`INSERT` session settings, so
    the only way these settings can reach an Iceberg engine write is through
    the table definition — and such a table would be permanently unwritable
    (every `INSERT` rejected by the write-time guard in
    `ParquetBlockOutputFormat`). `createStorageObjectStorage` therefore
    rejects the definition up front: `CREATE TABLE ... ENGINE = Iceberg*
    ... SETTINGS output_format_parquet_column_field_ids = ...` (or
    `output_format_parquet_auto_assign_field_ids = 1`) fails with
    `BAD_ARGUMENTS` at `CREATE` time. The write-time guard stays as defense in
    depth for tables attached from existing metadata.

    This path is not reachable from a stateless (`file()` / `clickhouse-local`)
    test because no Iceberg engine table is involved there.
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

    # A plain CREATE + INSERT works — the datalake column-id mapping is used.
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

    # Baking the auto-assign setting into the table definition is rejected at
    # CREATE time — accepting it would freeze the setting into the engine's
    # FormatSettings and make every subsequent INSERT fail.
    auto_table = make_table_name("auto")
    error = instance.query_and_get_error(
        get_creation_expression(
            storage_type,
            auto_table,
            started_cluster_iceberg_no_spark,
            "(x Int32)",
            format_version,
            additional_settings=["output_format_parquet_auto_assign_field_ids = 1"],
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    assert instance.query(f"EXISTS TABLE {auto_table}") == "0\n"

    # An explicit field-id override map in the table definition is rejected too.
    explicit_table = make_table_name("explicit")
    error = instance.query_and_get_error(
        get_creation_expression(
            storage_type,
            explicit_table,
            started_cluster_iceberg_no_spark,
            "(x Int32)",
            format_version,
            additional_settings=[
                "output_format_parquet_column_field_ids = {'x': '1'}"
            ],
        )
    )
    assert "BAD_ARGUMENTS" in error
    assert "column-id mapping" in error
    assert instance.query(f"EXISTS TABLE {explicit_table}") == "0\n"
