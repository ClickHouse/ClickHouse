#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RETRIES=5

# do not trust CLICKHOUSE_URL var because it contains randomization settings
CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

query="SELECT 'OK'"

echo "buffer_size=WRONG -- should fail"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?buffer_size=WRONG" -d "$query" | grep -o "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
echo "buffer_size=WRONG&buffer_size=0 -- WRONG value is overrided, last value is used"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?buffer_size=WRONG&buffer_size=0" -d "$query"


query="SELECT number as x, intDiv(10 - x, 10 - x) FROM numbers(100)"

echo "wait_end_of_query=0&http_wait_end_of_query=1 -- receive partial result end exception"
${CLICKHOUSE_CURL} -qsS "${CLICKHOUSE_URL}?max_threads=1&max_block_size=1&buffer_size=0&wait_end_of_query=0" -d "$query" 2>/dev/null | sed 's/DB::Exception://g'
echo "wait_end_of_query=0&http_wait_end_of_query=1 -- do not receive result, only exception. http_wait_end_of_query overrides wait_end_of_query"
${CLICKHOUSE_CURL} -qsS "${CLICKHOUSE_URL}?max_threads=1&max_block_size=1&buffer_size=0&wait_end_of_query=0&http_wait_end_of_query=1" -d "$query" | sed 's/DB::Exception://g'
