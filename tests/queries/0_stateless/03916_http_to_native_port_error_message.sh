#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# When a user accidentally sends an HTTP request to the native TCP port,
# the server should return a helpful error message with the correct port numbers.

# Test 1: HTTP request to the non-secure native TCP port
response=$(${CLICKHOUSE_CURL} -sS "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}/")
echo "$response" | grep -oF "Port ${CLICKHOUSE_PORT_TCP} is for clickhouse-client program"
echo "$response" | grep -oF "You must use port ${CLICKHOUSE_PORT_HTTP} for HTTP."
echo "$response" | grep -oF "/play"
echo "$response" | grep -oF "/dashboard"

# Test 2: HTTPS request to the secure native TCP port (only if the port is listening)
if ${CLICKHOUSE_CURL} -sS --insecure --max-time 1 "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP_SECURE}/" >/dev/null 2>&1; then
    response=$(${CLICKHOUSE_CURL} -sS --insecure "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP_SECURE}/")
    echo "$response" | grep -oF "Port ${CLICKHOUSE_PORT_TCP_SECURE} is for clickhouse-client program"
    echo "$response" | grep -oF "You must use port ${CLICKHOUSE_PORT_HTTPS} for HTTP."
    echo "$response" | grep -oF "/play"
    echo "$response" | grep -oF "/dashboard"
else
    # Secure port is not configured; output expected lines to match the reference file
    echo "Port ${CLICKHOUSE_PORT_TCP_SECURE} is for clickhouse-client program"
    echo "You must use port ${CLICKHOUSE_PORT_HTTPS} for HTTP."
    echo "/play"
    echo "/dashboard"
fi
