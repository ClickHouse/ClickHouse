#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan, no-ubsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

test_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04141_oom_canary_manual_sigkill.XXXXXX")
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
    <default_profile>default</default_profile>
    <background_schedule_pool_size>4</background_schedule_pool_size>
    <background_buffer_flush_schedule_pool_size>1</background_buffer_flush_schedule_pool_size>
    <background_pool_size>2</background_pool_size>
    <background_common_pool_size>2</background_common_pool_size>
    <background_move_pool_size>1</background_move_pool_size>
    <background_fetches_pool_size>1</background_fetches_pool_size>
    <tables_loader_background_pool_size>1</tables_loader_background_pool_size>
    <tables_loader_foreground_pool_size>1</tables_loader_foreground_pool_size>
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
    --oom_canary_relaunch true \
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

kill -9 "$canary_pid"

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

result=$("$CLICKHOUSE_BINARY" client "${client_args[@]}" --query "SELECT 1" 2>&1)
if [[ "$result" == "1" ]]; then
    echo "server alive after manual canary kill"
else
    echo "server not responding after manual canary kill" >&2
    exit 1
fi

if [[ -n "$new_canary_pid" ]]; then
    echo "canary relaunched"
else
    echo "canary relaunch not detected" >&2
    exit 1
fi

if grep -q "Skipping OOM response" "$server_log"; then
    echo "non-oom kill skipped"
else
    echo "manual canary kill was not classified as non-OOM" >&2
    tail -n 100 "$server_log" >&2
    exit 1
fi

if grep -q "Queued OOM canary event in system.crash_log" "$server_log"; then
    echo "unexpected crash_log event" >&2
    tail -n 100 "$server_log" >&2
    exit 1
else
    echo "no crash_log event queued"
fi

kill "$server_pid" 2>/dev/null
wait "$server_pid" 2>/dev/null
server_pid=""

