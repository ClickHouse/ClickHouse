import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_upload_directory
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_writes_different_path_format_error(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session

    TABLE_NAME = "test_writes_different_path_format_error_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id string) USING iceberg TBLPROPERTIES ('format-version' = '{format_version}')")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('maneskin');", settings={"allow_insert_into_iceberg": 1})
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('radiohead');", settings={"allow_insert_into_iceberg": 1, "write_full_path_in_iceberg_metadata": True})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL;") == 'maneskin\nradiohead\n'
