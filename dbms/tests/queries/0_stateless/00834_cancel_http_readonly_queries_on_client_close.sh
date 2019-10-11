#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} --max-time 1 -sS "${CLICKHOUSE_URL_PARAMS}&query_id=cancel_http_readonly_queries_on_client_close&cancel_http_readonly_queries_on_client_close=1&query=SELECT+count()+FROM+system.numbers" 2>&1 | grep -cF 'curl: (28)'

for i in {1..10}
do
    ${CLICKHOUSE_CURL} -sS --data "SELECT count() FROM system.processes WHERE query_id = 'cancel_http_readonly_queries_on_client_close'" "${CLICKHOUSE_URL_PARAMS}" | grep '0' && break
    sleep 0.2
done
