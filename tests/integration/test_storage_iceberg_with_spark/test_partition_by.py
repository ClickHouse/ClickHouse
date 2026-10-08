import pytest

from helpers.iceberg_utils import (
    default_upload_directory,
    write_iceberg_from_df,
    generate_data,
    create_iceberg_table,
    get_uuid_str
)


@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_partition_by(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = (
        "test_partition_by_"
        + format_version
        + "_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    write_iceberg_from_df(
        spark,
        generate_data(spark, 0, 10),
        TABLE_NAME,
        mode="overwrite",
        format_version=format_version,
        partition_by="a",
    )

    files = default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    assert len(files) == 14  # 10 partitions + 4 metadata files

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10
    assert int(instance.query(f"SELECT count() FROM system.iceberg_history WHERE table = '{TABLE_NAME}'")) == 1
