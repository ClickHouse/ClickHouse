#!/usr/bin/env bash
# Verify getClientInfo over the HTTP interface.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "-- Interface is HTTP when queried via HTTP."
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT getClientInfo('interface')"

echo "-- HTTP method is POST for -d requests."
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT getClientInfo('http_method')"

echo "-- User-Agent is populated for HTTP queries."
${CLICKHOUSE_CURL} -sS -A 'getClientInfo-test/1.0' "${CLICKHOUSE_URL}" -d "SELECT getClientInfo('http_user_agent')"

echo "-- current_user works over HTTP."
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT getClientInfo('current_user') = currentUser()"
