#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} --max-time 1 -sS "${CLICKHOUSE_URL}&query_id=cancel_http_readonly_queries_on_client_close&cancel_http_readonly_queries_on_client_close=1&query=SELECT+count()+FROM+system.numbers" 2>&1 | grep -cF 'curl: (28)'

while true
do
    ${CLICKHOUSE_CURL} -sS --data "SELECT count() FROM system.processes WHERE query_id = 'cancel_http_readonly_queries_on_client_close'" "${CLICKHOUSE_URL}" | grep '0' && break
    sleep 0.2
done

${CLICKHOUSE_CURL} -sS -X POST "${CLICKHOUSE_URL}&session_id=test_00834_session&readonly=2&cancel_http_readonly_queries_on_client_close=1" -d "CREATE TEMPORARY TABLE table_tmp AS SELECT 1 FORMAT JSON"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=test_00834_session&query=DROP+TEMPORARY+TABLE+table_tmp"

url_https="https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTPS}/?session_id=test_00834_session"
${CLICKHOUSE_CURL} -sSk -X POST "$url_https&readonly=2&cancel_http_readonly_queries_on_client_close=1" -d "CREATE TEMPORARY TABLE table_tmp AS SELECT 1 FORMAT JSON"
${CLICKHOUSE_CURL} -sSk "$url_https&session_id=test_00834_session&query=DROP+TEMPORARY+TABLE+table_tmp"
