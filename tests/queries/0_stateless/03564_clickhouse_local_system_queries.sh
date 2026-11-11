#!/usr/bin/env bash

# Test that clickhouse-local properly handles SYSTEM queries:
# - SYSTEM RELOAD CONFIG should throw UNSUPPORTED_METHOD
# - SYSTEM START/STOP LISTEN should work for TCP and HTTP
# - Various SYSTEM CLEAR CACHE queries should work

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# SYSTEM RELOAD CONFIG is not supported in clickhouse-local
$CLICKHOUSE_LOCAL --query "SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }"

# STOP LISTEN on a fresh clickhouse-local (no active listeners) should be a no-op
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTP;"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN TCP;"

# START LISTEN with port 0 (OS-assigned) should succeed without conflicting with existing servers.
# Specify a single --listen_host because port 0 with multiple listen_host values is rejected
# (the registry stores a single port per port_name).
$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --http_port 0 --query "SYSTEM START LISTEN HTTP;"
$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --query "SYSTEM START LISTEN TCP;"

# Test that a started TCP listener is actually reachable from outside the process.
# We use port 0 (OS-assigned) to avoid collisions with other tests running in parallel,
# and read the actual bound port from clickhouse-local's stdout via `getServerPort`.
LOCAL_STDOUT=$(mktemp --tmpdir clickhouse-local-stdout.XXXXXX)
LOCAL_STDERR=$(mktemp --tmpdir clickhouse-local-stderr.XXXXXX)
trap 'rm -f "$LOCAL_STDOUT" "$LOCAL_STDERR"' EXIT

$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --query "
    SYSTEM START LISTEN TCP;
    SELECT getServerPort('tcp_port') FORMAT TSV;
    SELECT sleepEachRow(1) FROM numbers(30) SETTINGS max_block_size = 1 FORMAT Null;
" > "$LOCAL_STDOUT" 2> "$LOCAL_STDERR" &
LOCAL_PID=$!

# Wait for the bound port to appear on stdout (up to 5 seconds).
LOCAL_TCP_PORT=""
for _ in $(seq 1 50); do
    if [[ -s "$LOCAL_STDOUT" ]]; then
        LOCAL_TCP_PORT=$(head -n 1 "$LOCAL_STDOUT")
        break
    fi
    if ! kill -0 "$LOCAL_PID" 2>/dev/null; then
        break
    fi
    sleep 0.1
done

if [[ -z "$LOCAL_TCP_PORT" || "$LOCAL_TCP_PORT" -eq 0 ]]; then
    echo "Failed to discover TCP listener port (clickhouse-local stderr below):" >&2
    cat "$LOCAL_STDERR" >&2
    kill "$LOCAL_PID" 2>/dev/null || true
    wait "$LOCAL_PID" 2>/dev/null || true
    exit 1
fi

# Connect from outside to confirm the listener is actually reachable.
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
    echo "Failed to connect to TCP listener on port $LOCAL_TCP_PORT (clickhouse-local stderr below):" >&2
    cat "$LOCAL_STDERR" >&2
    kill "$LOCAL_PID" 2>/dev/null || true
    wait "$LOCAL_PID" 2>/dev/null || true
    exit 1
fi

kill "$LOCAL_PID" 2>/dev/null || true
wait "$LOCAL_PID" 2>/dev/null || true

# SYSTEM START LISTEN with an invalid listen_host must fail, not silently succeed
$CLICKHOUSE_LOCAL --listen_host 192.0.2.1 --tcp_port 0 --query "SYSTEM START LISTEN TCP; -- { serverError NETWORK_ERROR }"

# Port 0 (OS-assigned) combined with multiple listen_host values is rejected to keep `getServerPort`
# unambiguous (one stored value per `port_name`). Without an explicit `--listen_host`, the default
# is to listen on both `::1` and `127.0.0.1`, which is the multi-host case we forbid for port 0.
$CLICKHOUSE_LOCAL --tcp_port 0 --query "SYSTEM START LISTEN TCP; -- { serverError BAD_ARGUMENTS }"
$CLICKHOUSE_LOCAL --http_port 0 --query "SYSTEM START LISTEN HTTP; -- { serverError BAD_ARGUMENTS }"

# SYSTEM START LISTEN must be idempotent: repeating it for an already-running listener is a no-op,
# matching clickhouse-server behavior.
$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --query "
    SYSTEM START LISTEN TCP;
    SYSTEM START LISTEN TCP;
    SELECT 'idempotent_tcp_ok';
"
$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --http_port 0 --query "
    SYSTEM START LISTEN HTTP;
    SYSTEM START LISTEN HTTP;
    SELECT 'idempotent_http_ok';
"

# SYSTEM START/STOP LISTEN for protocols that clickhouse-local does not manage
# (e.g. HTTPS, MYSQL, QUERIES CUSTOM) must fail with UNSUPPORTED_METHOD rather than
# a misleading NETWORK_ERROR (start path) or a silent no-op (stop path).
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN HTTPS; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN MYSQL; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN QUERIES CUSTOM; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTPS; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN MYSQL; -- { serverError UNSUPPORTED_METHOD }"

# Various SYSTEM CLEAR CACHE queries should work in clickhouse-local
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR DNS CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR MARK CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR UNCOMPRESSED CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR QUERY CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR SCHEMA CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM CLEAR FORMAT SCHEMA CACHE;"
