#!/usr/bin/env bash
# Tags: no-parallel
# Test that server settings can be passed as direct CLI options (without -- separator)

CLICKHOUSE_PORT_TCP=50222
CLICKHOUSE_DATABASE=default

# The test starts short-lived servers on the same port, one after another. Disable the watchdog so
# that each `$CLICKHOUSE_BINARY server` is a single process: `$!` is then the server itself (not a
# watchdog parent that outlives a still-running child), and `kill; wait` shuts it down and releases
# the port synchronously before the next server is started. Otherwise a lingering child would keep
# the port bound and the next server would fail with `No servers started`.
export CLICKHOUSE_WATCHDOG_ENABLE=0

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Starting a server is slow in the sanitizer builds, so only two servers are started that have to
# become ready, and every case that needs a running server is checked on one of them.
PID=

function finish()
{
    if [[ -n "$PID" ]]; then
        kill "$PID" 2>/dev/null
        wait "$PID" 2>/dev/null
    fi
}
trap finish EXIT

function wait_for_server()
{
    local log=$1
    for _ in {1..300}; do
        $CLICKHOUSE_CLIENT --query "SELECT 1" >/dev/null 2>&1 && return
        if ! kill -0 "$PID" 2>/dev/null; then
            break
        fi
        sleep 0.2
    done
    cat "$log"
    exit 1
}

function stop_server()
{
    kill "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    PID=
}

# Server 1. Several cases at once:
# - Direct CLI options (no -- separator) are applied (`max_thread_pool_size`, Bool `shutdown_wait_unfinished_queries`).
# - The value after the -- separator takes precedence over a direct option (`max_connections`), also for a
#   setting backed by a dotted config path (`openssl_server_cache_sessions` -> `openSSL.server.cacheSessions`),
#   and also when the dotted key itself is used after the separator (`openSSL.server.sessionTimeout`, the key of
#   `openssl_server_session_timeout`).
# - Settings whose name starts with an abbreviation of a built-in option are registered as direct options too.
#   Every letter of `c` (`config-file`), `d` (`daemon`), `e` (`errorlog-file`), `h` (`help`), `l` (`log-file`),
#   `u` (`umask`) and `v` (`version`) is a unique prefix of a built-in option, so skipping the colliding
#   settings would leave whole families - `http_*`, `dns_*`, `logger_*`, ... - unavailable.
# - A server setting whose name is also the name of a user-level setting (`query_cache_max_size_in_bytes`)
#   works as a direct option: `Settings::checkNoSettingNamesAtTopLevel` must not reject it at startup. The
#   same holds when its legacy nested spelling is used after the separator (`query_cache.max_entries`), which
#   must also override a direct option.
# - For a setting backed by a dotted config path, the components that read the raw configuration must see
#   the same value as `system.server_settings`: `logger_level` after the separator must actually take effect.
# - A built-in option that binds the config key of a path-backed setting is another spelling of that setting:
#   `--log-file` binds `logger.log` (the key of `logger_log`) and `-E` (`--errorlog-file`) binds
#   `logger.errorlog` (the key of `logger_errorlog`). The last occurrence must win across the spellings: the
#   built-in `--log-file` after `--logger_log`, and `--LOGGER_ERRORLOG:...` after `-E` - which also checks that
#   an option spelled in a different case and with the `:` separator, both accepted by `Poco`, is recognized.
srv_dir1="${CLICKHOUSE_TMP}/srv1"
mkdir -p "$srv_dir1"
$CLICKHOUSE_BINARY server \
    --max_thread_pool_size 999888 \
    --shutdown_wait_unfinished_queries 1 \
    --max_connections 30 \
    --openssl_server_cache_sessions 0 \
    --openssl_server_session_timeout 3 \
    --dns_max_consecutive_failures 42 \
    --http_connections_soft_limit 111 \
    --uncompressed_cache_size 1048576 \
    --concurrent_threads_soft_limit_num 7 \
    --load_marks_threadpool_pool_size 13 \
    --query_cache_max_entries 10 \
    --query_cache_max_size_in_bytes 2097152 \
    --logger_log "$srv_dir1/loser.log" --log-file "$srv_dir1/winner.log" \
    -E "$srv_dir1/loser.err" --LOGGER_ERRORLOG:"$srv_dir1/winner.err" \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir1/" \
    --max_connections 50 \
    --openssl_server_cache_sessions 1 \
    --openSSL.server.sessionTimeout 4 \
    --query_cache.max_entries 11 \
    --logger_level information > "${CLICKHOUSE_TMP}/server1.log" 2>&1 &
PID=$!
wait_for_server "${CLICKHOUSE_TMP}/server1.log"

# Path-backed settings are displayed in `system.server_settings` under their dotted path.
$CLICKHOUSE_CLIENT --query "
    SELECT name, if(name = 'shutdown_wait_unfinished_queries', toString(value IN ('1', 'true')), value)
    FROM system.server_settings
    WHERE name IN ('max_thread_pool_size', 'shutdown_wait_unfinished_queries', 'max_connections',
                   'openSSL.server.cacheSessions', 'openSSL.server.sessionTimeout',
                   'dns_max_consecutive_failures', 'http_connections_soft_limit', 'uncompressed_cache_size',
                   'concurrent_threads_soft_limit_num', 'load_marks_threadpool_pool_size',
                   'query_cache.max_entries', 'query_cache.max_size_in_bytes', 'logger.level')
    ORDER BY name"
$CLICKHOUSE_CLIENT --query "SELECT name, splitByChar('/', value)[-1] FROM system.server_settings WHERE name IN ('logger.log', 'logger.errorlog') ORDER BY name"

stop_server

for f in winner.log winner.err loser.log loser.err; do
    if [[ -e "$srv_dir1/$f" ]]; then
        echo "$f exists"
    else
        echo "$f does not exist"
    fi
done

# The default (embedded) configuration logs at the `trace` level, so if `logger.level` had not received
# the value from the command line, the log would contain `<Debug>` and `<Trace>` messages.
if grep -qE '<(Debug|Trace)>' "$srv_dir1/winner.log"; then
    echo "FAIL: logger.level is not applied"
else
    echo "OK: logger.level is applied"
fi

# Server 2: The `config_file` setting is backed by the `config-file` key, which is what
# `BaseDaemon::loadConfiguration` resolves the configuration file from. The flat spelling must therefore
# actually select the configuration file, also when it is given in a different case after the built-in
# `--config-file` option, and `system.server_settings` must report the config file the server really runs
# on. Use a config file with a distinctive `max_connections` value as the marker that it was genuinely loaded.
srv_dir2="${CLICKHOUSE_TMP}/srv2"
mkdir -p "$srv_dir2"
cfg2="$srv_dir2/custom_config.xml"
cat > "$cfg2" <<XML
<clickhouse>
    <logger>
        <level>trace</level>
        <console>true</console>
    </logger>
    <max_connections>777</max_connections>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <profiles>
        <default/>
    </profiles>
    <quotas>
        <default/>
    </quotas>
</clickhouse>
XML
$CLICKHOUSE_BINARY server \
    --config-file "$srv_dir2/no_such_config.xml" --CONFIG_FILE "$cfg2" \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir2/" > "${CLICKHOUSE_TMP}/server2.log" 2>&1 &
PID=$!
wait_for_server "${CLICKHOUSE_TMP}/server2.log"

$CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'max_connections'"
$CLICKHOUSE_CLIENT --query "SELECT value = '$cfg2' FROM system.server_settings WHERE name = 'config-file'"

stop_server

# The following servers fail at startup (or only print something and exit), so they are fast.

# Unknown option produces an error
srv_dir3="${CLICKHOUSE_TMP}/srv3"
mkdir -p "$srv_dir3"
$CLICKHOUSE_BINARY server \
    --no_such_server_setting 42 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir3/" > "${CLICKHOUSE_TMP}/server3.log" 2>&1

grep -o 'Unknown option specified: no_such_server_setting' "${CLICKHOUSE_TMP}/server3.log" | head -1

# Duplicate option detection
$CLICKHOUSE_BINARY server \
    --max_thread_pool_size 100 --max_thread_pool_size 200 \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir3/" > "${CLICKHOUSE_TMP}/server4.log" 2>&1

grep -o 'Option must not be given more than once: max_thread_pool_size' "${CLICKHOUSE_TMP}/server4.log" | head -1

# Every previously-unique built-in abbreviation must keep resolving after server settings are registered
# as CLI options. `--co` is an abbreviation of `--config-file`; registering `compiled_expression_cache_*`
# and `concurrent_threads_*` must not make it ambiguous. `--lo` is an abbreviation of `--log-file`;
# registering `load_marks_*` must not make it ambiguous either. Pass a non-existent config file so the
# server fails fast, and check that the failure is not ambiguous.
$CLICKHOUSE_BINARY server \
    --max_connections 10 --lo="$srv_dir3/server.log" --co="$srv_dir3/no_such_config.xml" \
    -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path "$srv_dir3/" > "${CLICKHOUSE_TMP}/server5.log" 2>&1

if grep -q 'Ambiguous option' "${CLICKHOUSE_TMP}/server5.log"; then
    echo "FAIL: --co or --lo is ambiguous"
else
    echo "OK: --co and --lo resolve"
fi

# An abbreviation of a built-in option keeps its meaning even when server settings starting with
# the same letter are registered: `--v` is `--version` (not an ambiguity with `validate_*`) and `--h` is
# `--help` (not an ambiguity with `http_*`).
$CLICKHOUSE_BINARY server --v 2>&1 | grep -c 'server version'
$CLICKHOUSE_BINARY server --h 2>&1 | grep -q -- '--max_thread_pool_size' && echo 1 || echo 0
