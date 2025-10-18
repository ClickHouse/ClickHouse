import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    get_uuid_str,
    create_iceberg_table
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_uuid_column(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
            "test_uuid_" + format_version + "_" + storage_type + "_" + get_uuid_str()
    )

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id UUID) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}', 'write.format.default' = '{format}')"
    )

    spark.sql(f"INSERT INTO {TABLE_NAME} select {get_uuid_str()}")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    check_schema_and_data(instance, TABLE_NAME, [], [])

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500
