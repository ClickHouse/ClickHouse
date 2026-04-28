#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan, no-ubsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04104_oom_canary_no_relaunch.XXXXXX")
mkdir -p "$test_dir/data" "$test_dir/tmp" "$test_dir/user_files" "$test_dir/format_schemas"
server_log="${test_dir}/server.log"

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

CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY \
    --config-file "$test_dir/config.xml" \
    -- \
    --path "$test_dir/data" \
    --tmp_path "$test_dir/tmp" \
    --user_files_path "$test_dir/user_files" \
    --format_schema_path "$test_dir/format_schemas" \
    --oom_canary_enable true \
    --allow_experimental_oom_canary true \
    --oom_canary_size 1048576 \
    --oom_canary_relaunch false \
    --logger.log "$server_log" \
    --logger.errorlog "$test_dir/error.log" \
    >& "$test_dir/stdout.log" &
server_pid=$!

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

tcp_port=$(grep -oP 'native protocol \(tcp\): [^:]+:\K[0-9]+' "$server_log" | head -1)
if [[ -z "$tcp_port" ]]; then
    echo "Cannot determine TCP port from server log" >&2
    cat "$server_log" >&2
    exit 1
fi

client_args=(--send_logs_level=warning --database=default --port "$tcp_port")

canary_pid=""
for child in $(pgrep -P "$server_pid" 2>/dev/null); do
    oom_adj=$(cat "/proc/$child/oom_score_adj" 2>/dev/null)
    if [[ "$oom_adj" == "1000" ]]; then
        canary_pid="$child"
        break
    fi
done

if [[ -z "$canary_pid" ]]; then
    echo "Cannot find OOM canary child process" >&2
    exit 1
fi

echo "canary found"

"$CLICKHOUSE_BINARY" client "${client_args[@]}" --query "SYSTEM ENABLE FAILPOINT oom_canary_force_oom_evidence"
kill -9 "$canary_pid"

monitor_exited=0
for _ in $(seq 1 100); do
    if grep -q "OOM canary monitor thread exiting" "$server_log" 2>/dev/null; then
        monitor_exited=1
        break
    fi
    sleep 0.1
done

if [[ "$monitor_exited" -ne 1 ]]; then
    echo "canary monitor did not exit" >&2
    tail -n 100 "$server_log" >&2
    exit 1
fi

result=$("$CLICKHOUSE_BINARY" client "${client_args[@]}" --query "SELECT 1" 2>&1)
if [[ "$result" == "1" ]]; then
    echo "server alive after canary kill"
else
    echo "server not responding after canary kill" >&2
    exit 1
fi

has_canary=0
for child in $(pgrep -P "$server_pid" 2>/dev/null); do
    oom_adj=$(cat "/proc/$child/oom_score_adj" 2>/dev/null)
    if [[ "$oom_adj" == "1000" ]]; then
        has_canary=1
        break
    fi
done

if [[ "$has_canary" -eq 0 ]]; then
    echo "canary not relaunched"
else
    echo "unexpected canary relaunch" >&2
    exit 1
fi

if grep -q "OOM canary relaunched with new pid" "$server_log"; then
    echo "unexpected relaunch log entry" >&2
    tail -n 100 "$server_log" >&2
    exit 1
fi

kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null
server_pid=""
