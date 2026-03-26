#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/97172
# When the Parquet V3 reader applies prewhere and filters out all rows,
# `read_rows` was reported as 0 in `system.query_log` instead of the actual number
# of rows physically read from the file.
#
# The bug requires two conditions:
#   1. The table must have multiple columns so the query optimizer pushes the
#      WHERE clause into prewhere (single-column tables skip prewhere since
#      there is no I/O savings from reading fewer columns first).
#   2. The prewhere filter must eliminate ALL rows, triggering the
#      `rows_pass == 0` early-break path in `ReadManager` that was not
#      accounting for the physically read rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_read_rows"
LOG_COMMENT="04061_read_rows_${CLICKHOUSE_DATABASE}"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# Create a local Iceberg table with two columns and 10 rows.
# Two columns are needed so the optimizer moves the WHERE on `s` to prewhere,
# reading `s` first and deferring `payload` until after filtering.
${CLICKHOUSE_CLIENT} -q "
    SET allow_experimental_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_TABLE_PATH}', 'Parquet', 's String, payload String')
    SELECT toString(number), randomPrintableASCII(100)
    FROM numbers(10);
"

# Read from the Iceberg table with a WHERE clause that matches no rows.
# All pushdown/pruning settings are disabled to match the original report;
# prewhere is independent of these and will still be applied by the optimizer.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM icebergLocal('${ICEBERG_TABLE_PATH}')
    WHERE s < '0'
    FORMAT Null
    SETTINGS
        log_comment = '${LOG_COMMENT}',
        input_format_parquet_filter_push_down = 0,
        input_format_parquet_bloom_filter_push_down = 0,
        input_format_parquet_page_filter_push_down = 0;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log;"

# read_rows should be 10, not 0
${CLICKHOUSE_CLIENT} -q "
    SELECT read_rows
    FROM system.query_log
    WHERE log_comment = '${LOG_COMMENT}'
      AND type = 'QueryFinish'
      AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"
