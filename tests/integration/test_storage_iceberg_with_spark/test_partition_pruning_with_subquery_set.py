import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

@pytest.mark.parametrize(
    "storage_type",
    ["s3", "azure", "local"],
)
def test_partition_pruning_with_subquery_set(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_partition_pruning_" + storage_type + "_" + get_uuid_str()
    IN_MEMORY_TABLE = "in_memory_table_" + get_uuid_str()

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
                data STRING
            )
            USING iceberg
            PARTITIONED BY (identity(id))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd'),
        (5, 'e');
    """
    )


    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    instance.query(f"CREATE TABLE {IN_MEMORY_TABLE} (id INT) ENGINE = Memory")
    instance.query(f"INSERT INTO {IN_MEMORY_TABLE} VALUES (2), (4)")


    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {
            "use_iceberg_partition_pruning": 0
        }
        settings2 = {
            "use_iceberg_partition_pruning": 1
        }
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, 'IcebergPartitionPrunedFiles', select_expression
        )

    assert (
        check_validity_and_get_prunned_files(
            f"SELECT * FROM {creation_expression} WHERE id in (SELECT id FROM {IN_MEMORY_TABLE}) ORDER BY ALL"
        )
        == 3
    )


