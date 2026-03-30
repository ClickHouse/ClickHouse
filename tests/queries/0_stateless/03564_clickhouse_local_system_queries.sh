#!/usr/bin/env bash

# Test that clickhouse-local properly handles SYSTEM queries:
# - SYSTEM RELOAD CONFIG should throw UNSUPPORTED_METHOD
# - SYSTEM START/STOP LISTEN should work for TCP and HTTP
# - Various SYSTEM CLEAR CACHE queries should work

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# SYSTEM RELOAD CONFIG is not supported in clickhouse-local
$CLICKHOUSE_LOCAL --query "SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }"

# STOP LISTEN on a fresh clickhouse-local (no active listeners) should be a no-op
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTP;"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN TCP;"

# START LISTEN with port 0 (OS-assigned) should succeed without conflicting with existing servers
$CLICKHOUSE_LOCAL --http_port 0 --query "SYSTEM START LISTEN HTTP;"
$CLICKHOUSE_LOCAL --tcp_port 0 --query "SYSTEM START LISTEN TCP;"

# Test that a started TCP listener is actually reachable:
# start clickhouse-local with a blocking query to keep it alive, then connect from outside.
$CLICKHOUSE_LOCAL --tcp_port 0 --query "
    SYSTEM START LISTEN TCP;
    SELECT sleepEachRow(3) FROM numbers(100) FORMAT Null;
" &
LOCAL_PID=$!

# Discover the actual listening port via lsof (wait up to 5 seconds)
PORT=''
for _ in $(seq 1 50); do
    PORT="$(lsof -n -a -P -i tcp -s tcp:LISTEN -p "$LOCAL_PID" 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1) }' | head -1)"
    if [[ -n "$PORT" ]]; then
        break
    fi
    sleep 0.1
done

if [[ -n "$PORT" ]]; then
    $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$PORT" --query "SELECT 'connected_ok'"
else
    echo "Failed to discover listening port" >&2
fi

kill "$LOCAL_PID" 2>/dev/null
wait "$LOCAL_PID" 2>/dev/null || true

# Various SYSTEM CLEAR CACHE queries should work in clickhouse-local
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR DNS CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR MARK CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR UNCOMPRESSED CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR QUERY CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR SCHEMA CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR FORMAT SCHEMA CACHE;"
