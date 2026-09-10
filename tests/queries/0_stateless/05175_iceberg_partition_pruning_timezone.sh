#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/119173:
# an Iceberg partition value is the UTC floor of the stored instant, but the pruner rebuilt the
# partition key with `toRelativeDayNum`/`toRelativeHourNum`, which floor in the timezone of the
# source column. Iceberg `timestamp` maps to a zone-less `DateTime64(6)`, so that timezone is the
# session's or the server's: under a non-UTC one the pruner derived a shifted partition value and
# silently skipped files that hold matching rows, with no error and no log line.
#
# The same query is asked in three shapes, because they do not share one partition key: the table as
# just created in this server, the same directory through the table function, and the same table
# after DETACH/ATTACH. All three must agree with the unpruned count.
#
# Every statement pins `session_timezone`, because the test runner randomizes that setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DAY_TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_day"
DAY_PATH="${USER_FILES_PATH}/${DAY_TABLE}/"
HOUR_TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_hour"
HOUR_PATH="${USER_FILES_PATH}/${HOUR_TABLE}/"

trap 'rm -rf "${DAY_PATH}" "${HOUR_PATH}" 2>/dev/null' EXIT

# Both rows fall in UTC day 19724 (2024-01-02), and in two different UTC hours. Background Iceberg
# compaction would rewrite the manifests these arms read, so it is pinned off per table.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${DAY_TABLE} (ts DateTime64(6), id Int32)
    ENGINE = IcebergLocal('${DAY_PATH}', 'Parquet') PARTITION BY (toRelativeDayNum(ts))
    SETTINGS allow_experimental_iceberg_compaction = 0;

    CREATE TABLE ${HOUR_TABLE} (ts DateTime64(6), id Int32)
    ENGINE = IcebergLocal('${HOUR_PATH}', 'Parquet') PARTITION BY (toRelativeHourNum(ts))
    SETTINGS allow_experimental_iceberg_compaction = 0;
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${DAY_TABLE} SETTINGS session_timezone = 'UTC' VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2);
    INSERT INTO ${HOUR_TABLE} SETTINGS session_timezone = 'UTC' VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2);
"

echo "--- rows as stored ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT ts, id FROM ${DAY_TABLE} ORDER BY id SETTINGS session_timezone = 'UTC' FORMAT TSV"

# 2024-01-03 05:00:00 in Asia/Tokyo is 2024-01-02 20:00:00Z, which is the second row. Pruning must
# agree with execution: both counts are 1.
echo "--- day transform, non-UTC session, table as created, pruning on then off ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts = '2024-01-03 05:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${DAY_TABLE} WHERE ts = '2024-01-03 05:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 0;"

echo "--- day transform, non-UTC session, same directory through the table function ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM icebergLocal('${DAY_PATH}') WHERE ts = '2024-01-03 05:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;"

echo "--- day transform, non-UTC session, same table after DETACH/ATTACH ---"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${DAY_TABLE}; ATTACH TABLE ${DAY_TABLE};"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts = '2024-01-03 05:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;"

echo "--- day transform, range predicate, non-UTC session ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts >= '2024-01-03 00:00:00' AND ts < '2024-01-03 09:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${DAY_TABLE} WHERE ts >= '2024-01-03 00:00:00' AND ts < '2024-01-03 09:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 0;"

echo "--- day transform, UTC session ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts = '2024-01-02 20:00:00'
        SETTINGS session_timezone = 'UTC', use_iceberg_partition_pruning = 1;"

# `toRelativeHourNum` returns t / 3600 for any timezone whose offset is a whole number of hours, so
# the hour transform only shifts in a zone with a fractional offset. Asia/Kolkata is +05:30, where
# 2024-01-03 01:30:00 is the same instant as 2024-01-02 20:00:00Z.
echo "--- hour transform, fractional-offset session, pruning on then off ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${HOUR_TABLE} WHERE ts = '2024-01-03 01:30:00'
        SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${HOUR_TABLE} WHERE ts = '2024-01-03 01:30:00'
        SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 0;"

echo "--- partition spec transform names ---"
grep -ho '"transform" : "[^"]*"' "${DAY_PATH}metadata/"*.json | sort -u
grep -ho '"transform" : "[^"]*"' "${HOUR_PATH}metadata/"*.json | sort -u

# A predicate no partition can satisfy: the count is 0 and files are still skipped. Without this
# arm every arm above would also pass if pruning simply stopped working.
echo "--- non-matching predicate: no rows, and files are pruned ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts = '2025-06-01 00:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1,
                 log_comment = '${CLICKHOUSE_DATABASE}_prune_probe';
    SYSTEM FLUSH LOGS query_log;"
${CLICKHOUSE_CLIENT} --query "
    SELECT max(ProfileEvents['IcebergPartitionPrunedFiles']) > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '${CLICKHOUSE_DATABASE}_prune_probe'
    SETTINGS enable_parallel_replicas = 0"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${DAY_TABLE} SYNC;
    DROP TABLE IF EXISTS ${HOUR_TABLE} SYNC;"
