import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)


@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
@pytest.mark.parametrize("partition_type", ["", "identity(x)", "icebergBucket(3, x)"])
def test_writes_mutate_delete(started_cluster_iceberg_with_spark, storage_type, partition_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_mutate_delete_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''
    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (456);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (999);", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = 'pudge1000-7';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n456\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '123';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n999\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '999';", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\n'

    if storage_type != "local":
        return
    initial_files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query("SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup")
    with pytest.raises(Exception):
        instance.query(f"ALTER TABLE {TABLE_NAME} DELETE WHERE x = '456';", settings={"allow_experimental_insert_into_iceberg": 1})

    files = default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert len(initial_files) == len(files)

    df = spark.read.format("iceberg").load(f"/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1
