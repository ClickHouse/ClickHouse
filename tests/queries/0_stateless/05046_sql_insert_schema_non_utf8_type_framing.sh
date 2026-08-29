#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
SINGLE_BLOCK="&max_threads=1&group_by_two_level_threshold=0&group_by_two_level_threshold_bytes=0&max_block_size=65535&max_bytes_before_external_sort=0&max_bytes_ratio_before_external_sort=0"

echo '--- JSONEachPacketString is rejected for SQLInsert schema with a non-UTF-8 type name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT CAST(1 AS Enum8('x\xFFy' = 1)) AS c FORMAT SQLInsert SETTINGS output_format_sql_insert_include_table_schema = 1" \
    | grep -o -m1 'is not compatible with the output format SQLInsert'

echo '--- EventStream base64-encodes SQLInsert schema with a non-UTF-8 type name'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=EventStream${SINGLE_BLOCK}" \
    -d "SELECT CAST(1 AS Enum8('x\xFFy' = 1)) AS c FORMAT SQLInsert SETTINGS output_format_sql_insert_include_table_schema = 1" \
    | awk '/^event: data$/ { getline; sub(/^data: /, ""); print }' | base64 -d \
    | cmp -s - <(${CLICKHOUSE_CURL} -sS "${URL}" \
        -d "SELECT CAST(1 AS Enum8('x\xFFy' = 1)) AS c FORMAT SQLInsert SETTINGS output_format_sql_insert_include_table_schema = 1") \
    && echo 'SQLInsert schema payload with a non-UTF-8 type name round-trips' || echo 'MISMATCH'
