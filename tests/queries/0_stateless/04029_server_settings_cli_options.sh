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

# Test 5: The built-in `--config` option (an abbreviation of `--config-file`, used by the systemd
# unit and by `clickhouse restart`) must keep resolving after server settings are registered as CLI
# options. Registering settings whose name shares the `config` prefix (`config_file`,
# `config_reload_interval_ms`) previously made `--config` ambiguous and prevented the server from
# starting. Pass a non-existent config file so the server fails fast, and check that the failure is
# "config file not found" rather than "ambiguous option".
srv_dir5="${CLICKHOUSE_TMP}/srv5"
mkdir -p "$srv_dir5"
$CLICKHOUSE_BINARY server \
    --max_connections 10 --config="$srv_dir5/no_such_config.xml" \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir5/" > "${CLICKHOUSE_TMP}/server5.log" 2>&1

if grep -q 'Ambiguous option' "${CLICKHOUSE_TMP}/server5.log"; then
    echo "FAIL: --config is ambiguous"
else
    echo "OK: --config resolves"
fi

# Test 6: The built-in `--log` option (an abbreviation of `--log-file`) must likewise keep resolving.
# Registering the `logger_*` server settings (which all share the `log` prefix) previously made `--log`
# ambiguous with `logger_level`, `logger_log`, ... Pass a non-existent config file so the server fails
# fast, and check that the failure is not "ambiguous option" for `--log`.
srv_dir6="${CLICKHOUSE_TMP}/srv6"
mkdir -p "$srv_dir6"
$CLICKHOUSE_BINARY server \
    --log="$srv_dir6/server.log" --config="$srv_dir6/no_such_config.xml" \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir6/" > "${CLICKHOUSE_TMP}/server6.log" 2>&1

if grep -q 'Ambiguous option' "${CLICKHOUSE_TMP}/server6.log"; then
    echo "FAIL: --log is ambiguous"
else
    echo "OK: --log resolves"
fi

# Test 7: Bool server settings are registered as CLI options too. Pass one with an explicit value and
# verify it is applied. (Whether Bool flags may be given without a value - like the client's program
# options - is a separate design decision and is not exercised here.)
srv_dir7="${CLICKHOUSE_TMP}/srv7"
mkdir -p "$srv_dir7"
$CLICKHOUSE_BINARY server \
    --shutdown_wait_unfinished_queries 1 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir7/" > "${CLICKHOUSE_TMP}/server7.log" 2>&1 &
PID=$!

trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" >/dev/null 2>&1 && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server7.log"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query "SELECT value IN ('1', 'true') FROM system.server_settings WHERE name = 'shutdown_wait_unfinished_queries'"

kill $PID 2>/dev/null
wait $PID 2>/dev/null
trap '' EXIT

# Test 8: Settings backed by a dotted config path (e.g. `distributed_ddl_pool_size` ->
# `distributed_ddl.pool_size`) work as direct CLI options, and the value after the `--` separator
# still takes precedence for them: the command-line arguments are stored under the flat setting
# name, which `loadSettingsFromConfig` reads before the dotted path. (The settings are chosen not
# to shadow a user-level setting name - a user-level setting at the top level of the configuration
# is rejected at startup - and not to enable a subsystem by making its config section appear.)
srv_dir8="${CLICKHOUSE_TMP}/srv8"
mkdir -p "$srv_dir8"
$CLICKHOUSE_BINARY server \
    --distributed_ddl_pool_size 7 --distributed_ddl_max_tasks_in_queue 1500 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir8/" --distributed_ddl_max_tasks_in_queue 2500 > "${CLICKHOUSE_TMP}/server8.log" 2>&1 &
PID=$!

trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null' EXIT

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" >/dev/null 2>&1 && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server8.log"
        exit 1
    fi
done

# Path-backed settings are displayed in `system.server_settings` under their dotted path
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'distributed_ddl.pool_size'"
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'distributed_ddl.max_tasks_in_queue'"

kill $PID 2>/dev/null
wait $PID 2>/dev/null
trap '' EXIT
