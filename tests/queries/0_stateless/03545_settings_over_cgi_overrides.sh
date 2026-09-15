#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RETRIES=5

CH_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

query="SELECT 'OK'"

echo "buffer_size=WRONG -- should fail"
${CLICKHOUSE_CURL} -sS "${CH_URL}?buffer_size=WRONG" -d "$query" | grep -o "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
echo "buffer_size=WRONG&buffer_size=0 -- WRONG value is overrided, last value is used"
${CLICKHOUSE_CURL} -sS "${CH_URL}?buffer_size=WRONG&buffer_size=0" -d "$query"


query="SELECT number as x, throwIf(number > 3) FROM numbers(100) format JSONCompactEachRow"

CH_URL="${CH_URL}?enable_analyzer=0&http_write_exception_in_output_format=1&max_block_size=1&output_format_parallel_formatting=0"

echo "wait_end_of_query=0 -- receive partial result and exception"
${CLICKHOUSE_CURL} -qsS "${CH_URL}&wait_end_of_query=0" -d "$query" | sed "s/(version .*)//" | sed 's/DB::Exception://g'

echo "wait_end_of_query=0&http_wait_end_of_query=1 -- do not receive result, only exception. http_wait_end_of_query overrides wait_end_of_query"
${CLICKHOUSE_CURL} -qsS "${CH_URL}&wait_end_of_query=0&http_wait_end_of_query=1" -d "$query" | sed "s/(version .*)//" | sed 's/DB::Exception://g'
