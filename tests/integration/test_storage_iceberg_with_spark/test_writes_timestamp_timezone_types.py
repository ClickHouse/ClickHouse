import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)

# 2023-12-31 22:00:00.000001 UTC.
MICROS = 1704060000000001
LOCAL = "2023-12-31 22:00:00.000001"


def write_and_download(started_cluster, storage_type, instance, table_name, data_format):
    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster,
        "(ts DateTime64(6), tsz DateTime64(6, 'UTC'))",
        format=data_format,
    )

    instance.query(
        f"INSERT INTO {table_name} SELECT fromUnixTimestamp64Micro({MICROS}), fromUnixTimestamp64Micro({MICROS})",
        settings={"allow_insert_into_iceberg": 1},
    )

    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    default_download_directory(started_cluster, storage_type, f"{path}/", f"{path}/")
    return path


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_timestamp_timezone_types_parquet(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_timestamp_timezone_parquet_" + storage_type + "_" + get_uuid_str()

    path = write_and_download(
        started_cluster_iceberg_with_spark, storage_type, instance, table_name, "Parquet"
    )

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {"ts": "timestamp_ntz", "tsz": "timestamp"}
    row = table.selectExpr("cast(ts as string) AS ts", "unix_micros(tsz) AS tsz").collect()[0]
    assert row["ts"] == LOCAL
    assert row["tsz"] == MICROS

    assert dict(spark.read.parquet(f"{path}/data").dtypes) == {"ts": "timestamp_ntz", "tsz": "timestamp"}


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_writes_timestamp_timezone_types_orc(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = "test_writes_timestamp_timezone_orc_" + storage_type + "_" + get_uuid_str()

    path = write_and_download(
        started_cluster_iceberg_with_spark, storage_type, instance, table_name, "ORC"
    )

    table = spark.read.format("iceberg").load(path)
    assert dict(table.dtypes) == {"ts": "timestamp_ntz", "tsz": "timestamp"}
    row = table.selectExpr("cast(ts as string) AS ts", "unix_micros(tsz) AS tsz").collect()[0]
    assert row["ts"] == LOCAL
    assert row["tsz"] == MICROS

    # `spark.read.orc` cannot parse ORC `timestamp with local time zone`, so the annotation of the
    # raw file is read back with ClickHouse: `timestamp_instant` becomes DateTime64(9, 'UTC').
    if storage_type == "local":
        described = instance.query(
            f"DESCRIBE file('iceberg_data/default/{table_name}/data/*.orc', ORC) FORMAT TSVRaw"
        )
        assert [line.split("\t")[:2] for line in described.strip().split("\n")] == [
            ["ts", "Nullable(DateTime64(9))"],
            ["tsz", "Nullable(DateTime64(9, 'UTC'))"],
        ]
