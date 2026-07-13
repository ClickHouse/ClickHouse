import pytest
import os

from helpers.iceberg_utils import (
    default_upload_directory,
    create_iceberg_table,
    get_uuid_str
)



@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_metadata_file_format_with_uuid(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_metadata_selection_with_uuid_"
        + format_version
        + "_"
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

    for i in range(50):
        os.rename(
            f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/v{i + 1}.metadata.json",
            f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/{str(i).zfill(5)}-{get_uuid_str()}.metadata.json",
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 500

