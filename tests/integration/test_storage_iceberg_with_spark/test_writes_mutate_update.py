import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)

@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
@pytest.mark.parametrize("partition_type", ["", "identity(x)", "icebergBucket(3, x)"])
def test_writes_mutate_update(started_cluster_iceberg_with_spark, storage_type, partition_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_mutate_update_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String, y Int32)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('456', 2);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n456\t2\n'
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('999', 3);", settings={"allow_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = '777' WHERE x = '123';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\t2\n777\t1\n999\t3\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = 'goshan dr' WHERE x = '777';", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '456\t2\n999\t3\ngoshan dr\t1\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} UPDATE x = 'pudge1000-7' WHERE y = 2;", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '999\t3\ngoshan dr\t1\npudge1000-7\t2\n'

    if storage_type == "azure":
        return
    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    df.sort()
    assert str(df) == "[Row(x='999', y=3), Row(x='goshan dr', y=1), Row(x='pudge1000-7', y=2)]"