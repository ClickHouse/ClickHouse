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
    """,
    settings={"allow_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, '2024-02-20',
        '2024-03-20 15:00:00',
        'vasilisa', 6);
    """,
    settings={"allow_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, '2025-03-20',
        '2024-04-30 14:00:00',
        'icebreaker', 7);
    """,
    settings={"allow_insert_into_iceberg": 1}
    )

    instance.query(
    f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, '2025-04-20',
        '2024-05-30 14:00:00',
        'iceberg', 8);
    """,
    settings={"allow_insert_into_iceberg": 1}
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