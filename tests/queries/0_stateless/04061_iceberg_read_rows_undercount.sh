#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/97172
# When the native Parquet reader v3 applies prewhere and filters out all rows,
# read_rows was reported as 0 in system.query_log instead of the actual number
# of rows physically read from the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOG_COMMENT="04061_read_rows_${CLICKHOUSE_DATABASE}"

# Create a Parquet file with 10 rows
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}/04061_test.parquet')
    SELECT toString(number) AS s FROM numbers(10)
    SETTINGS engine_file_truncate_on_insert = 1;
"

# Read from the Parquet file with a WHERE clause that matches no rows.
# With all pushdown/pruning settings disabled, read_rows should still be 10
# because the file must be fully scanned.
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM file('${CLICKHOUSE_DATABASE}/04061_test.parquet')
    WHERE s < '0'
    FORMAT Null
    SETTINGS
        log_comment = '${LOG_COMMENT}',
        input_format_parquet_filter_push_down = 0,
        input_format_parquet_bloom_filter_push_down = 0,
        input_format_parquet_page_filter_push_down = 0;
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS;"

# read_rows should be 10, not 0
${CLICKHOUSE_CLIENT} -q "
    SELECT read_rows
    FROM system.query_log
    WHERE log_comment = '${LOG_COMMENT}'
      AND type = 'QueryFinish'
      AND current_database = '${CLICKHOUSE_DATABASE}'
    ORDER BY event_time_microseconds DESC
    LIMIT 1;
"
