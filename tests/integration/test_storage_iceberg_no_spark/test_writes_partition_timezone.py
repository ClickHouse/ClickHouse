import logging

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_download_directory,
    get_uuid_str,
)


def _count_parquet_files(started_cluster, storage_type, table_name):
    files = default_download_directory(
        started_cluster,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{table_name}/",
    )
    return sum(1 for file in files if file.endswith(".parquet"))


@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_partition_timezone_utc(started_cluster_iceberg_no_spark, storage_type):
    """ClickHouse INSERT must honour iceberg_partition_timezone when computing day partitions.

    Two rows share the Asia/Istanbul calendar day but fall on different UTC days:
      2024-01-02 01:00 Istanbul = 2024-01-01 22:00 UTC  -> UTC day Jan 1
      2024-01-02 05:00 Istanbul = 2024-01-02 02:00 UTC  -> UTC day Jan 2

    With iceberg_partition_timezone='UTC', ChunkPartitioner must produce two data files.
    If the setting were ignored and session TZ used, both rows would land in one partition.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_writes_partition_tz_utc_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(datetime DateTime, value Int64)",
        format_version=2,
        partition_by="toRelativeDayNum(datetime)",
    )

    instance.query(
        f"""
        INSERT INTO {table_name} VALUES
            ('2024-01-02 01:00:00', 1),
            ('2024-01-02 05:00:00', 2)
        """,
        settings={
            "allow_insert_into_iceberg": 1,
            "session_timezone": "Asia/Istanbul",
            "iceberg_partition_timezone": "UTC",
        },
    )

    assert instance.query(
        f"SELECT datetime, value FROM {table_name} ORDER BY value",
        settings={"session_timezone": "Asia/Istanbul"},
    ) == "2024-01-02 01:00:00.000000\t1\n2024-01-02 05:00:00.000000\t2\n"

    parquet_count = _count_parquet_files(
        started_cluster_iceberg_no_spark, storage_type, table_name
    )
    logging.info("UTC partition write produced %s parquet files", parquet_count)
    assert parquet_count == 2


@pytest.mark.parametrize("storage_type", ["local"])
def test_writes_partition_timezone_session_default(
    started_cluster_iceberg_no_spark, storage_type
):
    """Empty iceberg_partition_timezone uses session timezone for write partitioning.

    Same two Istanbul-same-day rows as the UTC test must produce a single partition
    when the setting is unset, proving the write path follows session TZ by default.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]
    table_name = "test_writes_partition_tz_session_" + get_uuid_str()

    create_iceberg_table(
        storage_type,
        instance,
        table_name,
        started_cluster_iceberg_no_spark,
        "(datetime DateTime, value Int64)",
        format_version=2,
        partition_by="toRelativeDayNum(datetime)",
    )

    instance.query(
        f"""
        INSERT INTO {table_name} VALUES
            ('2024-01-02 01:00:00', 1),
            ('2024-01-02 05:00:00', 2)
        """,
        settings={
            "allow_insert_into_iceberg": 1,
            "session_timezone": "Asia/Istanbul",
        },
    )

    parquet_count = _count_parquet_files(
        started_cluster_iceberg_no_spark, storage_type, table_name
    )
    logging.info("Session-TZ partition write produced %s parquet files", parquet_count)
    assert parquet_count == 1
