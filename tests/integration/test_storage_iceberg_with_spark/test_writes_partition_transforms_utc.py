import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)

# 2023-12-31 22:00:00.000001 UTC, which is already 2024-01-01 in Asia/Kolkata.
MODERN_MICROS = 1704060000000001
MODERN_LOCAL = "2023-12-31 22:00:00.000001"
# 1969-12-31 00:00:00.000001 UTC, before the epoch every date transform counts from.
PRE_EPOCH_MICROS = -86399999999
PRE_EPOCH_LOCAL = "1969-12-31 00:00:00.000001"

EXPECTED_PARTITION_VALUES = {
    "icebergYear": ["53", "-1"],
    "icebergMonth": ["647", "-1"],
    "icebergDay": ["2023-12-31", "1969-12-31"],
    "icebergHour": ["473350", "-24"],
}


@pytest.mark.parametrize("storage_type", ["s3", "local"])
@pytest.mark.parametrize("transform", ["icebergYear", "icebergMonth", "icebergDay", "icebergHour"])
def test_writes_date_transform_partition_values_are_utc(
    started_cluster_iceberg_with_spark, storage_type, transform
):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    table_name = (
        "test_writes_date_transform_" + transform.lower() + "_" + storage_type + "_" + get_uuid_str()
    )

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_with_spark,
        "(ts DateTime64(6), i Int64)",
        2,
        f"({transform}(ts))",
    )

    instance.query(
        f"""
        INSERT INTO {table_name} VALUES
            (fromUnixTimestamp64Micro({MODERN_MICROS}), 1), (fromUnixTimestamp64Micro({PRE_EPOCH_MICROS}), 2)
        """,
        settings={"allow_insert_into_iceberg": 1, "session_timezone": "Asia/Kolkata"},
    )

    path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}"
    default_download_directory(started_cluster_iceberg_with_spark, storage_type, f"{path}/", f"{path}/")

    table = spark.read.format("iceberg").load(path)
    assert table.count() == 2

    for expected_i, local in ((1, MODERN_LOCAL), (2, PRE_EPOCH_LOCAL)):
        rows = table.filter(f"ts = to_timestamp_ntz('{local}')").collect()
        assert [row["i"] for row in rows] == [expected_i], f"Spark cannot find the row for {local}"

    partitions = spark.read.format("iceberg").load(f"{path}#partitions")
    written = sorted(str(row["partition"]["ts"]) for row in partitions.collect())
    assert written == sorted(EXPECTED_PARTITION_VALUES[transform])


def test_iceberg_date_transform_functions(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]

    assert instance.query(
        """
        SELECT
            icebergYear(toDate32('1969-05-05')), icebergMonth(toDate32('1969-01-01')),
            icebergDay(toDate32('1969-12-31')), icebergHour(toDateTime64('1969-12-31 23:00:00', 6, 'UTC'))
        """
    ) == "-1\t-12\t-1\t-1\n"

    assert instance.query(
        f"""
        SELECT icebergYear(ts), icebergMonth(ts), icebergDay(ts), icebergHour(ts)
        FROM (SELECT CAST(fromUnixTimestamp64Micro({MODERN_MICROS}) AS DateTime64(6)) AS ts)
        """,
        settings={"session_timezone": "Asia/Kolkata"},
    ) == "53\t647\t19722\t473350\n"

    assert instance.query(
        """
        SELECT toTypeName(icebergDay(toDate('2024-01-01'))), icebergYear(CAST(NULL AS Nullable(Date32)))
        """
    ) == "Int32\t\\N\n"

    assert "ILLEGAL_TYPE_OF_ARGUMENT" in instance.query_and_get_error(
        "SELECT icebergHour(toDate('2024-01-01'))"
    )
