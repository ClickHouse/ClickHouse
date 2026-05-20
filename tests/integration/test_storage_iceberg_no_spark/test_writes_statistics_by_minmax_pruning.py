import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_statistics_by_minmax_pruning(started_cluster_iceberg_no_spark, format_version, storage_type):
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_statistics_by_minmax_pruning_" + storage_type + "_" + get_uuid_str()

    schema = """
    (tag Int32,
    date Date32,
    ts DateTime,
    name String,
    number Int64)
    """
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, schema, format_version)

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, '2024-01-20',
        '2024-02-20 10:00:00',
        'vasya', 5);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, '2024-02-20',
        '2024-03-20 15:00:00',
        'vasilisa', 6);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, '2025-03-20',
        '2024-04-30 14:00:00',
        'icebreaker', 7);
    """
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, '2025-04-20',
        '2024-05-30 14:00:00',
        'iceberg', 8);
    """
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergMinMaxIndexPrunedFiles', select_expression
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} ORDER BY ALL"
        )
        == 0
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE date <= '2024-01-25' ORDER BY ALL"
        )
        == 3
    )
    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE ts <= timestamp('2024-03-20 14:00:00.000000') ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag == 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE tag <= 1 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name == 'vasilisa' ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE name < 'kek' ORDER BY ALL"
        )
        == 2
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number == 8 ORDER BY ALL"
        )
        == 3
    )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {TABLE_NAME} WHERE number <= 5 ORDER BY ALL"
        )
        == 3
    )


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_statistics_float32_roundtrip(started_cluster_iceberg_no_spark, format_version, storage_type):
    """Regression test: Float32 min/max stats must round-trip correctly through Iceberg manifest.

    Field stores Float32 as Float64 internally, so serialization must cast back to 4 bytes.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    TABLE_NAME = "test_writes_statistics_float32_roundtrip_" + storage_type + "_" + get_uuid_str()

    schema = """
    (id Int32,
    value Float32)
    """
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_no_spark, schema, format_version)

    # File 1: Float32 range [1.5, 2.5]
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, 1.5), (2, 2.5);"
    )
    # File 2: Float32 range [10.0, 20.0]
    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (3, 10.0), (4, 20.0);"
    )

    def check_pruned(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1,
            "input_format_parquet_bloom_filter_push_down": 0,
            "input_format_parquet_filter_push_down": 0,
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergMinMaxIndexPrunedFiles', select_expression
        )

    # Query for values near 1.5-2.5: should prune file 2 (range 10-20)
    assert (
        check_pruned(
            f"SELECT * FROM {TABLE_NAME} WHERE value > 1.0 AND value < 3.0 ORDER BY ALL"
        )
        == 1
    )

    # Query for values near 15: should prune file 1 (range 1.5-2.5)
    assert (
        check_pruned(
            f"SELECT * FROM {TABLE_NAME} WHERE value > 5.0 AND value < 25.0 ORDER BY ALL"
        )
        == 1
    )

    # Verify correctness: all rows returned
    result = instance.query(
        f"SELECT id FROM {TABLE_NAME} WHERE value > 1.0 AND value < 3.0 ORDER BY id"
    ).strip()
    assert result == "1\n2", f"Expected '1\\n2' but got: {repr(result)}"