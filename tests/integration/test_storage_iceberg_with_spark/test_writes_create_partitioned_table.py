import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_download_directory
)


@pytest.mark.parametrize("format_version", [1, 2])
@pytest.mark.parametrize("storage_type", ["s3", "azure", "local"])
@pytest.mark.parametrize("partition_type", ["y", "identity(y)", "(identity(y))", "icebergTruncate(3, y)", "(identity(y), icebergBucket(3, x))", "(x, y)"])
def test_writes_create_partitioned_table(started_cluster_iceberg_with_spark, format_version, storage_type, partition_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_create_partitioned_table_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(x String, y Int64)", format_version, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES ('123', 1);", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '123\t1\n'

    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    with open(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/metadata/version-hint.text", "wb") as f:
        f.write(b"2")

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1


@pytest.mark.parametrize("storage_type", ["s3", "local"])
@pytest.mark.parametrize("partition_type", ["toYearNumSinceEpoch(d)", "toMonthNumSinceEpoch(d)", "toRelativeDayNum(d)"])
def test_writes_date_column_with_time_transforms(started_cluster_iceberg_with_spark, storage_type, partition_type):
    """Test that Date columns work with year/month/day partition transforms (issue #86337)."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_writes_date_time_transforms_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark, "(id Int64, d Date)", 2, partition_type)

    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == ''

    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (1, '2025-08-28');", settings={"allow_insert_into_iceberg": 1})
    assert instance.query(f"SELECT * FROM {TABLE_NAME} ORDER BY ALL") == '1\t2025-08-28\n'

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )

    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 1


@pytest.mark.parametrize("storage_type", ["local"])
@pytest.mark.parametrize(
    "tz_settings",
    [
        {"iceberg_timezone_for_timestamptz": "Europe/Berlin"},
        {"iceberg_timezone_for_timestamptz": ""},
    ],
)
def test_writes_reject_non_utc_timestamptz_timezone(
    started_cluster_iceberg_with_spark, storage_type, tz_settings
):
    """Non-default iceberg_timezone_for_timestamptz must not leak into partition transforms / Avro typing."""
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    TABLE_NAME = "test_writes_reject_tz_" + storage_type + "_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        TABLE_NAME,
        started_cluster_iceberg_with_spark,
        "(id Int32, ts DateTime64(6, 'UTC'))",
        2,
        "toRelativeHourNum(ts)",
    )

    error = instance.query_and_get_error(
        f"INSERT INTO {TABLE_NAME} VALUES (1, toDateTime64('2024-01-01 23:30:00', 6, 'UTC'))",
        settings={"allow_insert_into_iceberg": 1, **tz_settings},
    )
    assert "iceberg_timezone_for_timestamptz = 'UTC'" in error

    instance.query(
        f"INSERT INTO {TABLE_NAME} VALUES (1, toDateTime64('2024-01-01 23:30:00', 6, 'UTC'))",
        settings={"allow_insert_into_iceberg": 1, "iceberg_timezone_for_timestamptz": "UTC"},
    )
    assert (
        instance.query(
            f"SELECT timezoneOf(ts) FROM {TABLE_NAME} LIMIT 1 "
            "SETTINGS iceberg_timezone_for_timestamptz='Europe/Berlin'"
        ).strip()
        == "Europe/Berlin"
    )
