#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate the query promptly even when the client is blocked
# in `open()` of an explicit `--server_logs_file` sink. A FIFO opened for writing parks the caller
# until a reader attaches, and the log stream is created lazily from `onLogData`, i.e. in the middle
# of the query, before any cancellation hook could be installed on the resulting descriptor.
# 04654_client_cancel_blocked_outfile_open covers the same gap for the primary `INTO OUTFILE` sink.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_logs_file.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_logs_file.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_ERR"
}
trap cleanup EXIT

# No reader is ever attached to this FIFO, so the client stays inside `open()` for the whole test.
mkfifo "$FIFO"

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_blocked_logs_file"

# `send_logs_level=trace` guarantees the server sends log packets, so the client creates the log
# stream - and blocks in the `open()` of the FIFO - while the query is still running.
$CLICKHOUSE_CLIENT --server_logs_file="$FIFO" --send_logs_level=trace \
    --query_id="$QUERY_ID" \
    --query "SELECT number, repeat('x', 100) FROM numbers(1000000000)
             FORMAT Null
             SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                      max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > /dev/null 2> "$CLIENT_ERR" &
CLIENT=$!

# Wait until the query is running, which means the client has started to receive packets from the
# server and is now waiting for the log file to become writable.
started=0
for _ in {0..120}
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'" 2>/dev/null)
    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -ge 1 ]
    then
        started=1
        break
    fi
    # If the client has already exited, it never reached the blocked state - fail explicitly.
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.5
done

if [ "$started" -ne 1 ]
then
    echo "FAIL: the query did not reach the running state"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
    exit 0
fi

# A single Ctrl+C must be enough to terminate the client.
kill -SIGINT "$CLIENT" 2>/dev/null

# Give the client a few seconds to terminate.
for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still running after Ctrl+C"
    kill -9 "$CLIENT" 2>/dev/null
else
    echo "OK: client terminated after Ctrl+C"
fi

# The wait must be abandoned as a cancellation. Without an interruptible `open()` the outcome
# depends on whether the platform restarts the interrupted system call: it either keeps the client
# stuck (checked above) or surfaces the signal as a bogus `Cannot open file ...: Interrupted system
# call` failure of the log file.
if grep -q "Cannot open file" "$CLIENT_ERR"
then
    echo "FAIL: reported as a failure to open the log file"
    cat "$CLIENT_ERR"
else
    echo "OK: reported as a cancellation"
fi
