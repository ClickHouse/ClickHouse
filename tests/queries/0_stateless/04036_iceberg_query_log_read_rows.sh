#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/97172
# `system.query_log.read_rows` was reported as 0 for Iceberg reads.
#
# Test 1 — verifies that a simple aggregate over an Iceberg table reports
#   read_rows > 0 (covers the missing `FileProgress` callback fix).
#
# Test 2 — verifies that rows filtered by PREWHERE inside the Parquet V3
#   reader are still counted in read_rows.  100 rows are inserted (values
#   0..99) and PREWHERE c0 < 50 filters half of them inside `ReadManager`.
#   read_rows must equal 100 (the full physical scan), not 50.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_04036"

rm -rf "${ICEBERG_TABLE_PATH}"

# Create a local Iceberg table with 100 rows (values 0..99).
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_04036 (c0 Int32) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}');
    INSERT INTO t_04036 SELECT number FROM numbers(100);
"

# Test 1: aggregate query that forces actual Parquet reads.
${CLICKHOUSE_CLIENT} --query "
    SELECT /* 04036_iceberg_read_rows_test */ sum(c0)
    FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    SETTINGS optimize_count_from_files = 0
" > /dev/null

# Test 2: PREWHERE filters rows inside the Parquet V3 reader.
# We use c0 < 50 rather than c0 < 0 because the latter would be eliminated
# entirely by row-group statistics (min=0 satisfies "not < 0"), so the row
# group would be skipped before any rows are scanned.  c0 < 50 straddles the
# statistics range (min=0, max=99), forcing a row-level PREWHERE evaluation.
${CLICKHOUSE_CLIENT} --query "
    SELECT /* 04036_iceberg_prewhere_test */ *
    FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    PREWHERE c0 < 50
    SETTINGS
        use_iceberg_partition_pruning = 0,
        input_format_parquet_filter_push_down = 0,
        input_format_parquet_use_native_reader_v3 = 1
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

# Verify test 2: read_rows = 100 (all physically scanned rows, not just the
# 50 that passed the PREWHERE filter).
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
