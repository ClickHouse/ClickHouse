#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test verifies that the OOM canary child process is running,
# can be killed (simulating OOM killer), and that the server responds
# correctly: stays alive, writes to crash_log, and relaunches the canary.
#
# Uses an isolated temporary server to avoid affecting parallel tests.

test_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04057_oom_canary.XXXXXX")
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

# Start a temporary server with OOM canary enabled and a small canary size for speed.
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY \
    --config-file "$test_dir/config.xml" \
    -- \
    --path "$test_dir/data" \
    --tmp_path "$test_dir/tmp" \
    --user_files_path "$test_dir/user_files" \
    --format_schema_path "$test_dir/format_schemas" \
    --oom_canary_enable true \
    --oom_canary_size 1048576 \
    --oom_canary_relaunch true \
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

# Parse the actual TCP port from the log
tcp_port=$(grep -oP 'native protocol \(tcp\): [^:]+:\K[0-9]+' "$server_log" | head -1)
if [[ -z "$tcp_port" ]]; then
    echo "Cannot determine TCP port from server log" >&2
    cat "$server_log" >&2
    exit 1
fi

fatal_message='was killed by signal 9'
relaunch_message='OOM canary relaunched with new pid'

# Step 1: Find the OOM canary child process.
# The canary is a direct child of the server with oom_score_adj=1000.
canary_pid=""
for child in $(pgrep -P "$server_pid" 2>/dev/null); do
    oom_adj=$(cat "/proc/$child/oom_score_adj" 2>/dev/null)
    if [[ "$oom_adj" == "1000" ]]; then
        canary_pid="$child"
        break
    fi
done

if [[ -z "$canary_pid" ]]; then
    echo "Cannot find OOM canary child process (child of server pid $server_pid with oom_score_adj=1000)" >&2
    exit 1
fi

echo "canary found"

# Step 2: Kill the canary to simulate OOM killer behavior
kill -9 "$canary_pid"

# Step 3: Wait for the server to detect the canary death, execute response, and relaunch.
# Poll until a new canary appears or timeout (max 10 seconds).
new_canary_pid=""
for _ in $(seq 1 100); do
    for child in $(pgrep -P "$server_pid" 2>/dev/null); do
        oom_adj=$(cat "/proc/$child/oom_score_adj" 2>/dev/null)
        if [[ "$oom_adj" == "1000" ]] && [[ "$child" != "$canary_pid" ]]; then
            new_canary_pid="$child"
            break 2
        fi
    done
    sleep 0.1
done

# Step 4: Verify the server is still alive and responding
result=$("$CLICKHOUSE_BINARY" client --send_logs_level=warning --database=default --port "$tcp_port" --query "SELECT 1" 2>&1)
if [[ "$result" == "1" ]]; then
    echo "server alive after canary kill"
else
    echo "server not responding after canary kill" >&2
    exit 1
fi

# Step 5: Verify the server log contains the fatal canary message.
if grep -q "$fatal_message" "$server_log"; then
    echo "fatal log entry found"
else
    echo "fatal log entry not found" >&2
    tail -n 100 "$server_log" >&2
    exit 1
fi

# Step 6: Check that the canary was relaunched
if [[ -n "$new_canary_pid" ]]; then
    echo "canary relaunched"
else
    echo "canary relaunch not detected" >&2
    exit 1
fi

if grep -q "$relaunch_message" "$server_log"; then
    echo "relaunch log entry found"
else
    echo "relaunch log entry not found" >&2
    tail -n 100 "$server_log" >&2
    exit 1
fi

# Clean shutdown
kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null
server_pid=""
