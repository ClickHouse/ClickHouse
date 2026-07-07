
import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_minmax_pruning_with_null(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_minmax_pruning_with_null" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark,
            started_cluster_iceberg_with_spark,
            storage_type,
            TABLE_NAME,
            query,
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                tag INT,
                date DATE,
                ts TIMESTAMP,
                time_struct struct<a : DATE, b : TIMESTAMP>,
                name VARCHAR(50),
                number BIGINT
            )
            USING iceberg
            OPTIONS('format-version'='2')
        """
    )

    # min-max value of time_struct in manifest file is null.
    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, DATE '2024-01-20',
        TIMESTAMP '2024-02-20 10:00:00', null, 'vasya', 5)
    """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (2, DATE '2024-02-20',
        TIMESTAMP '2024-03-20 15:00:00', null, 'vasilisa', 6)
    """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (3, DATE '2025-03-20',
        TIMESTAMP '2024-04-30 14:00:00', null, 'icebreaker', 7)
    """
    )
    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (4, DATE '2025-04-20',
        TIMESTAMP '2024-05-30 14:00:00', null, 'iceberg', 8)
    """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, DATE '2024-01-20',
        TIMESTAMP '2024-02-20 10:00:00', named_struct('a', DATE '2024-02-20', 'b', TIMESTAMP '2024-02-20 10:00:00'), 'vasya', 5)
    """
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
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
            f"SELECT * FROM {creation_expression} WHERE time_struct.a <= '2024-02-01' ORDER BY ALL"
        )
        == 1
    )
