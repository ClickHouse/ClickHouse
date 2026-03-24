#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for: system.query_log.read_rows = 0 for Iceberg reads
# Two related bugs are covered:
#
# Bug 1 (original): IcebergIterator::next() was not calling the FileProgress
# callback and IcebergDataObjectInfo was not populating ObjectMetadata.size_bytes.
# This caused read_rows = 0 for normal aggregate queries.
#
# Bug 2 (issue #97172): With Parquet native reader v3 (default since 26.2),
# PREWHERE is applied inside the format reader.  When every row is filtered out
# by PREWHERE the reader returns no chunks, so StorageObjectStorageSource never
# calls progress() and read_rows = 0 even though all rows were physically read.
# The fix tracks rows_total in ReadManager and reports the gap at file boundary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_04036"

# Cleanup from any previous run
rm -rf "${ICEBERG_TABLE_PATH}"

# Create a local Iceberg table and insert a known number of rows
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_04036 (c0 Int32) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_04036 SELECT number FROM numbers(100);
"

# --- Test 1: normal aggregate query forces actual Parquet file reads ---
# optimize_count_from_files = 0 prevents using manifest record_count.
${CLICKHOUSE_CLIENT} --query "
    SELECT /* 04036_iceberg_read_rows_test */ sum(c0)
    FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    SETTINGS optimize_count_from_files = 0
" > /dev/null

# --- Test 2: PREWHERE filters every row (issue #97172 regression) ---
# c0 values are 0..99, so PREWHERE c0 < 0 matches nothing.
# With use_iceberg_partition_pruning = 0 the manifest stats are not used to
# prune the file, and with input_format_parquet_filter_push_down = 0 the row
# group statistics are not used either.  The file IS opened and all 100 rows
# are physically read for PREWHERE evaluation, but no rows are returned to the
# pipeline.  Before the fix read_rows was 0; after the fix it is 100.
${CLICKHOUSE_CLIENT} --query "
    SELECT /* 04036_iceberg_prewhere_test */ *
    FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    PREWHERE c0 < 0
    SETTINGS
        use_iceberg_partition_pruning = 0,
        input_format_parquet_filter_push_down = 0
" > /dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# Verify test 1: read_rows > 0
${CLICKHOUSE_CLIENT} --query "
    SELECT read_rows > 0
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query LIKE '%04036_iceberg_read_rows_test%'
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
    ORDER BY event_time DESC
    LIMIT 1
"

# Verify test 2: read_rows equals the table row count (100) even though PREWHERE
# filtered every row.  Before the fix this returned 0.
${CLICKHOUSE_CLIENT} --query "
    SELECT read_rows
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query LIKE '%04036_iceberg_prewhere_test%'
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
    ORDER BY event_time DESC
    LIMIT 1
"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04036"
rm -rf "${ICEBERG_TABLE_PATH}"
