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
LOCAL_TCP_PORT=$((RANDOM % 10000 + 40000))

$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port "$LOCAL_TCP_PORT" --query "
    SYSTEM START LISTEN TCP;
    SELECT sleepEachRow(1) FROM numbers(30) SETTINGS max_block_size = 1 FORMAT Null;
" &
LOCAL_PID=$!

# Wait for the TCP listener to become reachable (up to 5 seconds)
CONNECTED=0
for _ in $(seq 1 50); do
    if $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$LOCAL_TCP_PORT" --query "SELECT 1" >/dev/null 2>&1; then
        CONNECTED=1
        break
    fi
    sleep 0.1
done

if [[ "$CONNECTED" -eq 1 ]]; then
    $CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$LOCAL_TCP_PORT" --query "SELECT 'connected_ok'"
else
    echo "Failed to connect to TCP listener on port $LOCAL_TCP_PORT" >&2
    exit 1
fi

kill "$LOCAL_PID" 2>/dev/null
wait "$LOCAL_PID" 2>/dev/null || true

# SYSTEM START LISTEN with an invalid listen_host must fail, not silently succeed
$CLICKHOUSE_LOCAL --listen_host 192.0.2.1 --tcp_port 0 --query "SYSTEM START LISTEN TCP; -- { serverError NETWORK_ERROR }"

# Various SYSTEM CLEAR CACHE queries should work in clickhouse-local
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR DNS CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR MARK CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR UNCOMPRESSED CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR QUERY CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR SCHEMA CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR FORMAT SCHEMA CACHE;"
