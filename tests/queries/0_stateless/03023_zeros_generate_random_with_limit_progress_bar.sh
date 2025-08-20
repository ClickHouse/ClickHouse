#!/usr/bin/env bash
# Tags: no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function run_with_progress_and_match_total_rows()
{
    CURL_RESPONSE=$(echo "$1" | \
            ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&http_wait_end_of_query=1&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&output_format_parallel_formatting=0" --data-binary @- 2>&1)

    echo "$CURL_RESPONSE" | grep -q '"total_rows_to_read":"100"' && echo "Matched" || echo "Expected total_rows_to_read not found: ${CURL_RESPONSE}"
}

run_with_progress_and_match_total_rows 'SELECT * FROM system.zeros LIMIT 100'
run_with_progress_and_match_total_rows 'SELECT * FROM system.zeros_mt LIMIT 100'
run_with_progress_and_match_total_rows "SELECT * FROM generateRandom('number UInt64') LIMIT 100"
