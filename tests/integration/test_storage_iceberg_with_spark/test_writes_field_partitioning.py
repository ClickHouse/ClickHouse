import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)
    
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_writes_field_partitioning(started_cluster_iceberg_with_spark, storage_type):
    format_version = 2
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_field_partitioning_" + storage_type + "_" + get_uuid_str()

    partition_spec = "(identity(id), identity(i32), identity(u32), identity(d), identity(d32), identity(i64), identity(u64), identity(dt), identity(s))"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(id UInt32,i32 Int32,u32 UInt32,d Date,d32 Date32,i64 Int64,u64 UInt64,dt DateTime,f32 Float32,f64 Float64,s String)", format_version, partition_spec)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"""
    INSERT INTO {TABLE_NAME} VALUES
    (
        1,
        -123,
        123,
        '2025-08-27',
        '2025-08-27',
        -123456789,
        123456789,
        '2025-08-27 12:34:56',
        3.14,
        2.718281828,
        '@capibarsci'
    );""", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\t-123\t123\t2025-08-27\t2025-08-27\t-123456789\t123456789\t2025-08-27 12:34:56.000000\t3.14\t2.718281828\t@capibarsci\n'
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
    assert str(df) == "[Row(id=1, i32=-123, u32=123, d=datetime.date(2025, 8, 27), d32=datetime.date(2025, 8, 27), i64=-123456789, u64=123456789, dt=datetime.datetime(2025, 8, 27, 12, 34, 56), f32=3.140000104904175, f64=2.718281828, s='@capibarsci')]"
