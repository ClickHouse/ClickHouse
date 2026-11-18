import pytest

from helpers.iceberg_utils import (
    get_uuid_str,
    default_upload_directory,
    create_iceberg_table
)

@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_explicit_metadata_file(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_explicit_metadata_file_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"CREATE TABLE {TABLE_NAME} (id bigint, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    )

    for i in range(50):
        spark.sql(
            f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10)"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path="")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path="metadata/v31.metadata.json")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 300

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path="metadata/v11.metadata.json")

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 100

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path=chr(0) + chr(1))
    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, explicit_metadata_path="../metadata/v11.metadata.json")
