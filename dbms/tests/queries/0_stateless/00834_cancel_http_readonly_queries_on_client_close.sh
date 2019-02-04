#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh


${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query_id=cancel_http_readonly_queries_on_client_close&cancel_http_readonly_queries_on_client_close=1&query=SELECT+count()+FROM+system.numbers" &
REQUEST_CURL_PID=$!
sleep 0.1

# Check query is registered
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes where query_id='cancel_http_readonly_queries_on_client_close'"

# Kill client (curl process)
kill -SIGTERM $REQUEST_CURL_PID
sleep 0.1

# Check query is killed after client is gone
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes where query_id='cancel_http_readonly_queries_on_client_close'"
