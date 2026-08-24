import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_creation_expression,
    get_uuid_str
)


@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_relevant_iceberg_schema_chosen(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_relevant_iceberg_schema_chosen_" + storage_type + "_" + get_uuid_str()
    
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            a INT NOT NULL
        ) using iceberg
        TBLPROPERTIES ('format-version' = '2',
            'commit.manifest.min-count-to-merge' = '1',
            'commit.manifest-merge.enabled' = 'true');
        """
    )

    values_list = ", ".join(["(1)" for _ in range(5)])
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES {values_list};
        """
    )

    spark.sql(
    f"""
        ALTER TABLE {TABLE_NAME} ADD COLUMN b INT;
    """
    )

    values_list = ", ".join(["(1, 2)" for _ in range(5)])
    spark.sql(
        f"""
        INSERT INTO {TABLE_NAME} VALUES {values_list};
        """
    )

    spark.sql(f"CALL system.rewrite_manifests('{TABLE_NAME}')")

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

    instance.query(f"SELECT * FROM {table_creation_expression} WHERE b >= 2", settings={"input_format_parquet_filter_push_down": 0, "input_format_parquet_bloom_filter_push_down": 0})
