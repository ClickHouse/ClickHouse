#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if [ ! -d /proc/pressure ]; then
    exit 0
fi

if ! "$CLICKHOUSE_BINARY" help 2>&1 | grep -qF 'clickhouse keeper [args]'; then
    exit 0
fi

TMP_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/keeper_psi_metrics_setting.XXXXXX")"
KEEPER_PID=""

function cleanup()
{
    if [ -n "$KEEPER_PID" ] && kill -0 "$KEEPER_PID" 2>/dev/null; then
        kill "$KEEPER_PID" 2>/dev/null || true
        wait "$KEEPER_PID" 2>/dev/null || true
    fi

    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

CONFIG="${TMP_DIR}/keeper_config.xml"
LOG="${TMP_DIR}/keeper.log"
ERROR_LOG="${TMP_DIR}/keeper.err.log"
STDOUT_LOG="${TMP_DIR}/keeper.stdout.log"

cat > "$CONFIG" <<EOF
<clickhouse>
    <listen_host>127.0.0.1</listen_host>
    <listen_try>true</listen_try>
    <os_collect_psi_metrics>false</os_collect_psi_metrics>
    <asynchronous_metrics_update_period_s>1</asynchronous_metrics_update_period_s>

    <logger>
        <level>trace</level>
        <log>${LOG}</log>
        <errorlog>${ERROR_LOG}</errorlog>
    </logger>

    <keeper_server>
        <server_id>1</server_id>
        <log_storage_path>${TMP_DIR}/coordination/log</log_storage_path>
        <snapshot_storage_path>${TMP_DIR}/coordination/snapshots</snapshot_storage_path>
        <cgroups_memory_observer_wait_time>0</cgroups_memory_observer_wait_time>

        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>15000</session_timeout_ms>
            <force_sync>false</force_sync>
            <raft_logs_level>warning</raft_logs_level>
        </coordination_settings>

        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>127.0.0.1</hostname>
                <port>0</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
EOF

"$CLICKHOUSE_BINARY" keeper --config="$CONFIG" --log-file="$LOG" --errorlog-file="$ERROR_LOG" > "$STDOUT_LOG" 2>&1 &
KEEPER_PID=$!

for _ in $(seq 1 300); do
    if grep -qF 'Ready for connections.' "$LOG" 2>/dev/null; then
        break
    fi

    if ! kill -0 "$KEEPER_PID" 2>/dev/null; then
        echo "Keeper exited before startup" >&2
        cat "$STDOUT_LOG" "$ERROR_LOG" "$LOG" >&2 2>/dev/null || true
        exit 1
    fi

    sleep 0.1
done

if ! grep -qF 'Ready for connections.' "$LOG" 2>/dev/null; then
    echo "Keeper did not become ready" >&2
    cat "$STDOUT_LOG" "$ERROR_LOG" "$LOG" >&2 2>/dev/null || true
    exit 1
fi

if ! kill -0 "$KEEPER_PID" 2>/dev/null; then
    echo "Keeper exited after startup" >&2
    cat "$STDOUT_LOG" "$ERROR_LOG" "$LOG" >&2 2>/dev/null || true
    exit 1
fi

if ! FD_LIST="$(find "/proc/${KEEPER_PID}/fd" -maxdepth 1 -type l -exec readlink {} \; 2>/dev/null)"; then
    echo "Cannot inspect Keeper file descriptors" >&2
    cat "$STDOUT_LOG" "$ERROR_LOG" "$LOG" >&2 2>/dev/null || true
    exit 1
fi

if ! kill -0 "$KEEPER_PID" 2>/dev/null; then
    echo "Keeper exited while checking file descriptors" >&2
    cat "$STDOUT_LOG" "$ERROR_LOG" "$LOG" >&2 2>/dev/null || true
    exit 1
fi

if echo "$FD_LIST" | grep -qF '/proc/pressure/'; then
    echo "Keeper opened PSI files despite os_collect_psi_metrics=false" >&2
    echo "$FD_LIST" | grep -F '/proc/pressure/' >&2
    exit 1
fi
