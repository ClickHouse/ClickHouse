import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_bool_and_small_integer_types(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_bool_and_small_integer_types_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(b Bool, nb Nullable(Bool), ab Array(Bool), i8 Int8, u8 UInt8, i16 Int16, u16 UInt16)",
        2,
        "b",
    )

    instance.query(
        f"INSERT INTO {table_name} VALUES (true, NULL, [true, false], -128, 255, -32768, 65535), "
        "(false, true, [], 127, 0, 32767, 0)",
        settings={"allow_insert_into_iceberg": 1},
    )

    assert instance.query(f"SELECT * FROM {table_name} ORDER BY i8") == (
        "true\t\\N\t[true,false]\t-128\t255\t-32768\t65535\n"
        "false\ttrue\t[]\t127\t0\t32767\t0\n"
    )

    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{path}/", f"{path}/")

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {
        "b": "boolean",
        "nb": "boolean",
        "ab": "array<boolean>",
        "i8": "int",
        "u8": "int",
        "i16": "int",
        "u16": "int",
    }
    rows = [tuple(row) for row in table.orderBy("i8").collect()]
    assert rows == [
        (True, None, [True, False], -128, 255, -32768, 65535),
        (False, True, [], 127, 0, 32767, 0),
    ]
    # The `true` partition value is decoded from the ClickHouse-written manifest.
    assert [tuple(row) for row in table.filter("b = true").collect()] == [
        (True, None, [True, False], -128, 255, -32768, 65535)
    ]
