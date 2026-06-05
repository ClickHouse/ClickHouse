#!/usr/bin/env bash
# Tags: no-parallel
# Test that server settings can be passed as direct CLI options (without -- separator)

CLICKHOUSE_PORT_TCP=50222
CLICKHOUSE_DATABASE=default

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

srv_dir="${CLICKHOUSE_TMP}/srv1"
mkdir -p "$srv_dir"

# Test 1: Direct CLI options (no -- separator)
# Pass multiple server settings directly, verify their values via system.server_settings
$CLICKHOUSE_BINARY server \
    --max_thread_pool_size 999888 \
    --max_connections 123 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir/" > "${CLICKHOUSE_TMP}/server1.log" 2>&1 &
PID=$!

function finish()
{
    kill $PID 2>/dev/null
    wait $PID 2>/dev/null
}
trap finish EXIT

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" >/dev/null 2>&1 && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server1.log"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_connections'"

kill $PID 2>/dev/null
wait $PID 2>/dev/null
trap '' EXIT

# Test 2: Unknown option produces an error
srv_dir2="${CLICKHOUSE_TMP}/srv2"
mkdir -p "$srv_dir2"
$CLICKHOUSE_BINARY server \
    --no_such_server_setting 42 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir2/" > "${CLICKHOUSE_TMP}/server2.log" 2>&1

grep -o 'Unknown option specified: no_such_server_setting' "${CLICKHOUSE_TMP}/server2.log" | head -1

# Test 3: Duplicate option detection
srv_dir3="${CLICKHOUSE_TMP}/srv3"
mkdir -p "$srv_dir3"
$CLICKHOUSE_BINARY server \
    --max_thread_pool_size 100 --max_thread_pool_size 200 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir3/" > "${CLICKHOUSE_TMP}/server3.log" 2>&1

grep -o 'Option must not be given more than once: max_thread_pool_size' "${CLICKHOUSE_TMP}/server3.log" | head -1

# Test 4: Option passed both as direct CLI and after -- separator
# The value after -- should take precedence
srv_dir4="${CLICKHOUSE_TMP}/srv4"
mkdir -p "$srv_dir4"
$CLICKHOUSE_BINARY server \
    --max_connections 30 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir4/" --max_connections 50 > "${CLICKHOUSE_TMP}/server4.log" 2>&1 &
PID=$!

trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" >/dev/null 2>&1 && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server4.log"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_connections'"

kill $PID 2>/dev/null
wait $PID 2>/dev/null
trap '' EXIT
