#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/96737
# cancel_http_readonly_queries_on_client_close should work with HTTPS connections
# that are closed gracefully (with TLS close_notify).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="cancel_https_readonly_$(date +%s)_$$"

# Send a long-running query over HTTPS with a 1-second timeout.
# curl will perform a graceful TLS shutdown (sending close_notify) when the timeout expires.
${CLICKHOUSE_CURL} -sSk --max-time 1 "${CLICKHOUSE_URL_HTTPS}&query_id=${query_id}&cancel_http_readonly_queries_on_client_close=1&max_rows_to_read=0&query=SELECT+count()+FROM+system.numbers" 2>&1 | grep -cF 'curl: (28)'

# Wait for the query to disappear from system.processes, confirming it was cancelled.
i=0 retries=300
while [[ $i -lt $retries ]]; do
    result=$(${CLICKHOUSE_CURL} -sS --data "SELECT count() FROM system.processes WHERE query_id = '${query_id}'" "${CLICKHOUSE_URL}")
    if [ "$result" = "0" ]; then
        echo "0"
        break
    fi
    ((++i))
    sleep 0.2
done

if [[ $i -ge $retries ]]; then
    echo "Query was not cancelled after ${retries} retries"
fi
