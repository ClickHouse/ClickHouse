#!/usr/bin/env bash

# Verify that CRLF characters in user-supplied HTTP parameters (like query_id)
# cannot be used to inject additional HTTP response headers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

extract_query_id() {
    grep '< X-ClickHouse-Query-Id:' | sed 's/\r$//' | sed 's/^< X-ClickHouse-Query-Id: //'
}

count_injected_headers() {
    grep -F -ci "< $1:" | sed 's/^0$/no injected headers/' | sed '/^[1-9]/s/.*/FAIL: injected header detected/'
}

echo "--- CRLF in query_id via URL parameter ---"
RESPONSE=$(${CLICKHOUSE_CURL} -sS --globoff -v "${CLICKHOUSE_URL}&query=SELECT+1&query_id=inject%0d%0aLocation:%20evil.com" 2>&1)
echo "$RESPONSE" | extract_query_id
echo "$RESPONSE" | count_injected_headers Location

echo "--- Normal query_id ---"
${CLICKHOUSE_CURL} -sS --globoff -v "${CLICKHOUSE_URL}&query=SELECT+1&query_id=normal_test_id_04055" 2>&1 \
    | extract_query_id
