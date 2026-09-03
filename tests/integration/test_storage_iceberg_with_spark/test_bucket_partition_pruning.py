import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_bucket_partition_pruning(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

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
                id INT,
                name STRING,
                value DECIMAL(10, 2),
                created_at DATE,
                event_time TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (bucket(3, id), bucket(2, name), bucket(4, value), bucket(5, created_at), bucket(3, event_time))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'Alice', 10.50, DATE '2024-01-20', TIMESTAMP '2024-01-20 10:00:00'),
        (2, 'Bob', 20.00, DATE '2024-01-21', TIMESTAMP '2024-01-21 11:00:00'),
        (3, 'Charlie', 30.50, DATE '2024-01-22', TIMESTAMP '2024-01-22 12:00:00'),
        (4, 'Diana', 40.00, DATE '2024-01-23', TIMESTAMP '2024-01-23 13:00:00'),
        (5, 'Eve', 50.50, DATE '2024-01-24', TIMESTAMP '2024-01-24 14:00:00');
        """
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1
        }
        return check_validity_and_get_prunned_files_general(
            instance,
            TABLE_NAME,
            settings1,
            settings2,
            "IcebergPartitionPrunedFiles",
            select_expression,
        )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    queries = [
        f"SELECT * FROM {creation_expression} WHERE id == 1 ORDER BY ALL",
        f"SELECT * FROM {creation_expression} WHERE value == 20.00 OR event_time == '2024-01-24 14:00:00' ORDER BY ALL",
        f"SELECT * FROM {creation_expression} WHERE id == 3 AND name == 'Charlie' ORDER BY ALL",
        f"SELECT * FROM {creation_expression} WHERE (event_time == TIMESTAMP '2024-01-21 11:00:00' AND name == 'Bob') OR (name == 'Eve' AND id == 5) ORDER BY ALL",
    ]

    for query in queries:
        assert check_validity_and_get_prunned_files(query) > 0
