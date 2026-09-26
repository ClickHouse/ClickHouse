import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str
)

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_minmax_pruning_for_arrays_and_maps_subfields_disabled(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_disable_minmax_pruning_for_arrays_and_maps_subfields_"
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
            id BIGINT,
            measurements ARRAY<DOUBLE>
            ) USING iceberg
            TBLPROPERTIES (
            'write.metadata.metrics.max' = 'measurements.element',
            'write.metadata.metrics.min' = 'measurements.element'
            );
        """
    )

    execute_spark_query(
        f"""
            INSERT INTO {TABLE_NAME} VALUES
            (1, array(23.5, 24.1, 22.8, 25.3, 23.9)),
            (2, array(18.2, 19.5, 17.8, 20.1, 19.3, 18.7)),
            (3, array(30.0, 31.2, 29.8, 32.1, 30.5, 29.9, 31.0)),
            (4, array(15.5, 16.2, 14.8, 17.1, 16.5)),
            (5, array(27.3, 28.1, 26.9, 29.2, 28.5, 27.8, 28.3, 27.6));
        """
    )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    table_creation_expression = get_creation_expression(
        storage_type,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        table_function=True,
    )

    table_select_expression = table_creation_expression

    instance.query(f"SELECT * FROM {table_select_expression} ORDER BY ALL")
