import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_uint32_widens_to_long(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_uint32_widens_to_long_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(u32 UInt32, i32 Int32)",
    )

    instance.query(
        f"INSERT INTO {table_name} SELECT 4294967290, -5",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT u32, i32 FROM {table_name}") == "4294967290\t-5\n"

    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{path}/", f"{path}/")

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {"u32": "bigint", "i32": "int"}
    row = table.collect()[0]
    assert (row["u32"], row["i32"]) == (4294967290, -5)


def test_writes_reject_uint64(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    table_name = "test_writes_reject_uint64_" + get_uuid_str()

    assert "BAD_ARGUMENTS" in instance.query_and_get_error(
        get_creation_expression(
            "local", table_name, started_cluster_iceberg_with_spark, "(x UInt64)"
        )
    )
