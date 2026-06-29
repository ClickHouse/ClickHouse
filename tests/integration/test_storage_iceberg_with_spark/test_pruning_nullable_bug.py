import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3"])
def test_pruning_nullable_bug(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_bucket_partition_pruning_" + storage_type + "_" + get_uuid_str()

    spark.sql(f"""
        CREATE TABLE {TABLE_NAME} (c0 STRING)
        USING iceberg
    """)

    spark.sql(f"""
        INSERT INTO {TABLE_NAME} VALUES ('Pasha Technick'), (NULL)
    """)
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME} WHERE c0 IS NULL;")) == 1
    assert instance.query(f"SELECT * FROM {TABLE_NAME} WHERE c0 IS NULL;") == '\\N\n'

