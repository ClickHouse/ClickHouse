#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test verifies that the OOM canary is NOT spawned when oom_canary_enable=false.
# We start a temporary server instance with the setting disabled and verify
# that no canary child process appears.

test_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04058_oom_canary_disabled.XXXXXX")
mkdir -p "$test_dir/data" "$test_dir/tmp" "$test_dir/user_files" "$test_dir/format_schemas"
server_log="${test_dir}/server.log"

# Write a minimal config that only opens a random TCP port.
cat > "$test_dir/config.xml" << XMLEOF
<clickhouse>
    <tcp_port>0</tcp_port>
    <listen_host>127.0.0.1</listen_host>
    <logger>
        <level>information</level>
    </logger>
    <mark_cache_size>0</mark_cache_size>
    <uncompressed_cache_size>0</uncompressed_cache_size>
    <shutdown_wait_unfinished>0</shutdown_wait_unfinished>
    <users_config>$test_dir/users.xml</users_config>
</clickhouse>
XMLEOF

cat > "$test_dir/users.xml" << 'XMLEOF'
<clickhouse>
    <profiles>
        <default/>
    </profiles>
    <users>
        <default>
            <password/>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
        </default>
    </users>
</clickhouse>
XMLEOF

function cleanup()
{
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null
        wait "$server_pid" 2>/dev/null
    fi
    rm -rf "$test_dir"
}
trap cleanup EXIT

# Start a temporary server with oom_canary disabled.
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY \
    --config-file "$test_dir/config.xml" \
    -- \
    --path "$test_dir/data" \
    --tmp_path "$test_dir/tmp" \
    --user_files_path "$test_dir/user_files" \
    --format_schema_path "$test_dir/format_schemas" \
    --oom_canary_enable false \
    --logger.log "$server_log" \
    --logger.errorlog "$test_dir/error.log" \
    >& "$test_dir/stdout.log" &
server_pid=$!

# Wait for the server to start (max 30 seconds)
started=0
for _ in $(seq 1 300); do
    if grep -q "Ready for connections" "$server_log" 2>/dev/null; then
        started=1
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "Server process died during startup" >&2
        cat "$server_log" >&2
        exit 1
    fi
    sleep 0.1
done

if [[ "$started" -ne 1 ]]; then
    echo "Server did not start in time" >&2
    cat "$server_log" >&2
    exit 1
fi

# Check that the log contains "OOM canary is disabled"
if grep -q "OOM canary is disabled" "$server_log"; then
    echo "canary disabled confirmed"
else
    echo "did not find 'OOM canary is disabled' in server log" >&2
    grep -i "canary" "$server_log" >&2
    exit 1
fi

# Check that no child process has oom_score_adj=1000
has_canary=0
for child in $(pgrep -P "$server_pid" 2>/dev/null); do
    oom_adj=$(cat "/proc/$child/oom_score_adj" 2>/dev/null)
    if [[ "$oom_adj" == "1000" ]]; then
        has_canary=1
        break
    fi
done

if [[ "$has_canary" -eq 0 ]]; then
    echo "no canary child process"
else
    echo "unexpected canary child process found" >&2
    exit 1
fi

# Clean shutdown
kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null
server_pid=""
