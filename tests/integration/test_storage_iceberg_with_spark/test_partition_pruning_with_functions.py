import pytest

from helpers.iceberg_utils import (
    check_validity_and_get_prunned_files_general,
    execute_spark_query_general,
    get_creation_expression,
    get_uuid_str,
)


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_partition_pruning_with_functions(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_partition_pruning_with_functions_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, query
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (
                ts_day TIMESTAMP,
                ts_hour TIMESTAMP,
                ts_month TIMESTAMP,
                d_year DATE,
                tag INT
            )
            USING iceberg
            PARTITIONED BY (days(ts_day), hours(ts_hour), months(ts_month), years(d_year))
            OPTIONS('format-version'='2')
        """
    )

    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (TIMESTAMP '2024-01-20 10:00:00', TIMESTAMP '2024-01-20 10:00:00', TIMESTAMP '2024-01-20 10:00:00', DATE '2024-01-20', 1),
        (TIMESTAMP '2024-01-21 11:00:00', TIMESTAMP '2024-01-20 11:00:00', TIMESTAMP '2024-02-20 10:00:00', DATE '2025-01-20', 2),
        (TIMESTAMP '2024-02-20 10:00:00', TIMESTAMP '2024-01-21 10:00:00', TIMESTAMP '2024-03-20 10:00:00', DATE '2026-01-20', 3),
        (TIMESTAMP '2025-02-20 10:00:00', TIMESTAMP '2024-01-21 11:00:00', TIMESTAMP '2025-01-20 10:00:00', DATE '2027-01-20', 4);
    """
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {"use_iceberg_partition_pruning": 0, "session_timezone": "UTC"}
        settings2 = {"use_iceberg_partition_pruning": 1, "session_timezone": "UTC"}
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, "IcebergPartitionPrunedFiles", select_expression
        )

    def select(where):
        return f"SELECT * FROM {creation_expression} WHERE {where} ORDER BY ALL"

    # A filter that wraps the partition source column in a monotonic function must still be able to
    # use the partition value, which is what https://github.com/ClickHouse/ClickHouse/issues/103433
    # reported for `toDate`.
    assert check_validity_and_get_prunned_files(select("toDate(ts_day) = toDate('2024-01-20')")) == 3
    assert check_validity_and_get_prunned_files(select("toStartOfDay(ts_day) = toDateTime64('2024-01-20 00:00:00', 6)")) == 3
    assert check_validity_and_get_prunned_files(select("toYYYYMMDD(ts_day) = 20240120")) == 3
    assert check_validity_and_get_prunned_files(select("toDate(ts_day) IN (toDate('2024-01-20'), toDate('2024-02-20'))")) == 2
    assert check_validity_and_get_prunned_files(select("toDate(ts_day) > toDate('2024-02-01')")) == 2
    assert check_validity_and_get_prunned_files(select("toYear(ts_day) = 2025")) == 3
    assert check_validity_and_get_prunned_files(select("toStartOfHour(ts_hour) = toDateTime64('2024-01-20 10:00:00', 6)")) == 3
    assert check_validity_and_get_prunned_files(select("toDate(ts_hour) = toDate('2024-01-21')")) == 2
    assert check_validity_and_get_prunned_files(select("toStartOfMonth(ts_month) = toDate('2024-02-01')")) == 3
    assert check_validity_and_get_prunned_files(select("toYYYYMM(ts_month) = 202503")) == 4
    assert check_validity_and_get_prunned_files(select("toYear(d_year) = 2026")) == 3
    assert check_validity_and_get_prunned_files(select("toStartOfYear(d_year) = toDate('2028-01-01')")) == 4

    # A constant that is not aligned to the transform's granularity: 10:30 is inside the 10:00 hour,
    # so the file of that hour must survive, and the same for a day and a month boundary.
    assert check_validity_and_get_prunned_files(select("toStartOfHour(ts_hour) < toDateTime64('2024-01-20 10:30:00', 6)")) == 3
    assert check_validity_and_get_prunned_files(select("toStartOfHour(ts_hour) >= toDateTime64('2024-01-20 10:30:00', 6)")) == 1
    assert check_validity_and_get_prunned_files(select("toDate(ts_day) < toDate('2024-01-20') + INTERVAL 12 HOUR")) == 3
    assert check_validity_and_get_prunned_files(select("toStartOfMonth(ts_month) > toDate('2024-02-10')")) == 2

    # A single day is one weekday, so the partition value answers this too: only 2024-01-20 is a Saturday.
    assert check_validity_and_get_prunned_files(select("toDayOfWeek(ts_day) = 6")) == 3

    # The partition value of a `day` transform says nothing about the hour of the day, so a filter on
    # it must not prune anything.
    assert check_validity_and_get_prunned_files(select("toHour(ts_day) = 10")) == 0


@pytest.mark.parametrize("storage_type", ["s3", "local"])
def test_partition_pruning_with_functions_before_epoch(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_partition_pruning_before_epoch_" + storage_type + "_" + get_uuid_str()

    def execute_spark_query(query: str):
        return execute_spark_query_general(
            spark, started_cluster_iceberg_with_spark, storage_type, TABLE_NAME, query
        )

    execute_spark_query(
        f"""
            CREATE TABLE {TABLE_NAME} (ts TIMESTAMP, d DATE, tag INT)
            USING iceberg
            PARTITIONED BY (days(ts), years(d))
            OPTIONS('format-version'='2')
        """
    )

    # The transforms count from 1970, so these partition values are negative.
    execute_spark_query(
        f"""
        INSERT INTO {TABLE_NAME} VALUES
        (TIMESTAMP '1969-11-15 07:00:00', DATE '1969-11-15', 1),
        (TIMESTAMP '1969-12-31 23:00:00', DATE '1970-01-01', 2),
        (TIMESTAMP '1970-01-01 00:30:00', DATE '1971-06-01', 3),
        (TIMESTAMP '2024-01-20 10:00:00', DATE '2024-01-20', 4);
    """
    )

    creation_expression = get_creation_expression(
        storage_type, TABLE_NAME, started_cluster_iceberg_with_spark, table_function=True
    )

    def check_validity_and_get_prunned_files(select_expression):
        settings1 = {"use_iceberg_partition_pruning": 0, "session_timezone": "UTC"}
        settings2 = {"use_iceberg_partition_pruning": 1, "session_timezone": "UTC"}
        return check_validity_and_get_prunned_files_general(
            instance, TABLE_NAME, settings1, settings2, "IcebergPartitionPrunedFiles", select_expression
        )

    def select(where):
        return f"SELECT * FROM {creation_expression} WHERE {where} ORDER BY ALL"

    assert check_validity_and_get_prunned_files(select("toDate32(ts) = toDate32('1969-11-15')")) == 3
    assert check_validity_and_get_prunned_files(select("toDate32(ts) >= toDate32('1970-01-01')")) == 2
    assert check_validity_and_get_prunned_files(select("toDate32(ts) < toDate32('1970-01-01')")) == 2
    assert check_validity_and_get_prunned_files(select("toYear(d) = 1969")) == 3
    assert check_validity_and_get_prunned_files(select("toYear(d) >= 1971")) == 2
