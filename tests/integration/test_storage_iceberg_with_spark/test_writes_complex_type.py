import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory
)

@pytest.mark.parametrize("format_version", ["1", "2"])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
def test_writes_complex_types(started_cluster_iceberg_with_spark, format_version, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_complex_types_" + storage_type + "_" + get_uuid_str()

    schema = "(x Array(Nullable(Int32)), z Map(Int32, Nullable(Int64)), y Tuple(zip Nullable(Int32), foo Nullable(Int32)))"
    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version)

    with pytest.raises(Exception):
        create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version)

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, schema, format_version, "", True)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    map_value = "{5:6}"
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ([1,2], {map_value}, (3,4));", settings={"allow_experimental_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '[1,2]\t{5:6}\t(3,4)\n'

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"1")

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1