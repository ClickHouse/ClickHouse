import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory,
)

@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "local", "azure"])
def test_writes_schema_evolution(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_schema_evolution_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x Nullable(Int32))", format_version)
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    with pytest.raises(Exception):
        instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Int64;", settings={"allow_experimental_insert_into_iceberg": 1})

    instance.query(f"ALTER TABLE {TABLE_NAME} MODIFY COLUMN x Nullable(Int64);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert '`x` Nullable(Int64)' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} ADD COLUMN y Nullable(Float64);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t\\N\n'
    assert '`y` Nullable(Float64)' in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (124, 4.56);", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t\\N\n124\t4.5600000000000005\n'

    instance.query(f"ALTER TABLE {TABLE_NAME} DROP COLUMN x;", settings={"allow_experimental_insert_into_iceberg": 1})
    assert '`x` Nullable(Int64)' not in instance.query(f"SHOW CREATE TABLE {TABLE_NAME}")

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '4.5600000000000005\n\\N\n'

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 2