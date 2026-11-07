#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/119173: an Iceberg `timestamp`
# column has no time zone in its type name, so the type object carries whichever zone was current when
# it was built. Execution parses a string literal against such a column in the session zone, while key
# analysis converts the same literal with a CAST to that type. When the two zones differ the pruner
# looks for the partition value of a different instant than the one execution matches, and silently
# skips the file that holds the matching row: no error, no log line. `use_iceberg_partition_pruning`
# is on by default.
#
# ClickHouse now writes `DateTime` and `DateTime64` columns as Iceberg `timestamptz`, which is read
# back as `DateTime64(6, 'UTC')`, so a string literal is parsed in UTC whatever the session zone is.
# The first part of the test covers such a table: its non-UTC arms name the instant with a constant in
# that zone, so the pruner and execution must agree on one instant expressed in a zone other than the
# column's. The second part covers the issue itself on a table whose column really is a `timestamp`,
# as in a table written by another engine or by an older ClickHouse version, with string literals.
#
# Each arm that can lose a row is paired with the same query at `use_iceberg_partition_pruning = 0`.
# That control returns 1, so the row is present in the file and only pruning removes it; without the
# control an arm cannot tell a pruning bug from a predicate that names the wrong instant.
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
#
# The creating session pins the zone: a `SETTINGS` clause on `CREATE ... ENGINE = IcebergLocal(...)`
# is read as storage settings, hence the client flag.
${CLICKHOUSE_CLIENT} --session_timezone UTC --query "
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

# The arms below are only meaningful while the column's zone stays UTC instead of following the
# session: if that ever changes, this prints the session zone and the test fails here rather than
# turning every arm into a tautology.
echo "--- zone the timestamp column carries, read under a non-UTC session ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT timeZoneOf(ts) FROM ${DAY_TABLE} SETTINGS session_timezone = 'Asia/Tokyo'"

# 2024-01-03 05:00:00 in Asia/Tokyo is 2024-01-02 20:00:00Z, which is the second row. Pruning must
# agree with execution: both counts are 1.
echo "--- day transform, non-UTC session, equality, pruning on then off ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE} WHERE ts = toDateTime64('2024-01-03 05:00:00', 6, 'Asia/Tokyo')
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${DAY_TABLE} WHERE ts = toDateTime64('2024-01-03 05:00:00', 6, 'Asia/Tokyo')
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 0;"

echo "--- day transform, non-UTC session, range, pruning on then off ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${DAY_TABLE}
        WHERE ts >= toDateTime64('2024-01-03 00:00:00', 6, 'Asia/Tokyo') AND ts < toDateTime64('2024-01-03 09:00:00', 6, 'Asia/Tokyo')
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${DAY_TABLE}
        WHERE ts >= toDateTime64('2024-01-03 00:00:00', 6, 'Asia/Tokyo') AND ts < toDateTime64('2024-01-03 09:00:00', 6, 'Asia/Tokyo')
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
    SELECT count() FROM ${HOUR_TABLE} WHERE ts = toDateTime64('2024-01-03 01:30:00', 6, 'Asia/Kolkata')
        SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 1;
    SELECT count() FROM ${HOUR_TABLE} WHERE ts = toDateTime64('2024-01-03 01:30:00', 6, 'Asia/Kolkata')
        SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 0;"

echo "--- partition spec transform names ---"
grep -hoE '"transform"[[:space:]]*:[[:space:]]*"[^"]*"' "${DAY_PATH}metadata/"*.json | tr -d ' ' | sort -u
grep -hoE '"transform"[[:space:]]*:[[:space:]]*"[^"]*"' "${HOUR_PATH}metadata/"*.json | tr -d ' ' | sort -u

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

# A `timestamp` column, as in a table written by another engine or by an older ClickHouse version. It
# is produced by rewriting the metadata file before any data is written, so the manifest files never
# carry a conflicting `timestamptz` copy of the schema. Every step runs in a separate clickhouse-local
# process, so no step can see metadata cached by an earlier one. The process zone is UTC, so the
# column type is built in UTC, while the arms below run in other session zones.
NTZ_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_ntz"
NTZ_DAY_PATH="${NTZ_DIR}/day/"
NTZ_HOUR_PATH="${NTZ_DIR}/hour/"
rm -rf "${NTZ_DIR}"
mkdir -p "${NTZ_DAY_PATH}" "${NTZ_HOUR_PATH}"
trap 'rm -rf "${DAY_PATH}" "${HOUR_PATH}" "${NTZ_DIR}" 2>/dev/null' EXIT

LOCAL="${CLICKHOUSE_LOCAL} --session_timezone UTC --allow_insert_into_iceberg=1 --async_insert=0"

${LOCAL} --query "
    CREATE TABLE d (ts DateTime64(6), id Int32)
    ENGINE = IcebergLocal('${NTZ_DAY_PATH}', 'Parquet') PARTITION BY (toRelativeDayNum(ts))
    SETTINGS allow_experimental_iceberg_compaction = 0;

    CREATE TABLE h (ts DateTime64(6), id Int32)
    ENGINE = IcebergLocal('${NTZ_HOUR_PATH}', 'Parquet') PARTITION BY (toRelativeHourNum(ts))
    SETTINGS allow_experimental_iceberg_compaction = 0;
"
sed -i 's/"timestamptz"/"timestamp"/g' "${NTZ_DAY_PATH}"metadata/*.metadata.json "${NTZ_HOUR_PATH}"metadata/*.metadata.json

ATTACH_NTZ="
    CREATE TABLE d ENGINE = IcebergLocal('${NTZ_DAY_PATH}', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0;
    CREATE TABLE h ENGINE = IcebergLocal('${NTZ_HOUR_PATH}', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0;"

${LOCAL} --query "${ATTACH_NTZ}
    INSERT INTO d SETTINGS session_timezone = 'UTC' VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2);
    INSERT INTO h SETTINGS session_timezone = 'UTC' VALUES ('2024-01-02 03:00:00', 1), ('2024-01-02 20:00:00', 2);
"

# The same arms as the original regression test, in one process, after the column type is built.
# The zone check guards them: if the column followed the session, every arm would be a tautology.
# 2024-01-03 05:00:00 in Asia/Tokyo and 2024-01-03 01:30:00 in Asia/Kolkata are both 2024-01-02 20:00:00Z.
${LOCAL} --query "${ATTACH_NTZ}
    SELECT '--- timestamp column: Iceberg type, rows as stored ---';
    SELECT toTypeName(ts), ts, id FROM d ORDER BY id SETTINGS session_timezone = 'UTC';
    SELECT '--- timestamp column: zone it carries, read under a non-UTC session ---';
    SELECT DISTINCT timeZoneOf(ts) FROM d SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT '--- timestamp column: day transform, non-UTC session, equality, pruning on then off ---';
    SELECT count() FROM d WHERE ts = '2024-01-03 05:00:00' SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM d WHERE ts = '2024-01-03 05:00:00' SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 0;
    SELECT '--- timestamp column: day transform, non-UTC session, range, pruning on then off ---';
    SELECT count() FROM d WHERE ts >= '2024-01-03 00:00:00' AND ts < '2024-01-03 09:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 1;
    SELECT count() FROM d WHERE ts >= '2024-01-03 00:00:00' AND ts < '2024-01-03 09:00:00'
        SETTINGS session_timezone = 'Asia/Tokyo', use_iceberg_partition_pruning = 0;
    SELECT '--- timestamp column: day transform, UTC session ---';
    SELECT count() FROM d WHERE ts = '2024-01-02 20:00:00' SETTINGS session_timezone = 'UTC', use_iceberg_partition_pruning = 1;
    SELECT '--- timestamp column: hour transform, fractional-offset session, pruning on then off ---';
    SELECT count() FROM h WHERE ts = '2024-01-03 01:30:00' SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 1;
    SELECT count() FROM h WHERE ts = '2024-01-03 01:30:00' SETTINGS session_timezone = 'Asia/Kolkata', use_iceberg_partition_pruning = 0;
"
