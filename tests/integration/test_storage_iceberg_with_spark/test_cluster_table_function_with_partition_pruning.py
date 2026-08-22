import pytest

from helpers.iceberg_utils import (
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

@pytest.mark.parametrize("format_version", ["2"])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_cluster_table_function_with_partition_pruning(
    started_cluster_iceberg_with_spark, format_version, storage_type
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = (
        "test_cluster_table_function_with_partition_pruning_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

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
            DROP TABLE IF EXISTS {TABLE_NAME};
        """
    )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                a int,
                b float
            )
            USING iceberg
            PARTITIONED BY (identity(a))
            OPTIONS ('format-version'='{format_version}')
        """
    )

    execute_spark_query(f"INSERT INTO {TABLE_NAME} VALUES (1, 1.0), (2, 2.0), (3, 3.0)")

    table_function_expr_cluster = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
        run_on_cluster=True,
    )

    instance.query(f"SELECT * FROM {table_function_expr_cluster} WHERE a = 1")

