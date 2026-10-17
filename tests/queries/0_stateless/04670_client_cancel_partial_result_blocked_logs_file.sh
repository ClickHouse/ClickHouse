#!/usr/bin/env bash
# Tags: no-fasttest

# An explicit `--server_logs_file` is an auxiliary sink: it carries diagnostics, not the result of
# the query. When its `open()` is blocked (a FIFO with no reader) and the query runs with
# `partial_result_on_first_cancel`, the first Ctrl+C must break the client out of that `open()`
# *and* still reach the server as a stage-one `Cancel`, so that the partial result is delivered
# through the result sink. Turning the interrupted `open()` into a client-side cancellation instead
# would silently downgrade the first signal to a hard cancel and lose the partial result.
# 04658_client_cancel_blocked_logs_file_open covers the same blocked sink with the default
# single-signal cancellation.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_logs_file.fifo"
CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_logs_file.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_logs_file.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

# No reader is ever attached to this FIFO, so the client stays inside `open()` of the log sink.
mkfifo "$FIFO"

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_partial_result_logs_file"

# `send_logs_level=trace` guarantees the server sends log packets, so the client creates the log
# stream - and blocks in the `open()` of the FIFO - while the query is still running. The result
# itself is a running aggregate, which is exactly what a stage-one `Cancel` returns as the partial
# result, and it goes to stdout, i.e. to a sink that is perfectly healthy.
$CLICKHOUSE_CLIENT --server_logs_file="$FIFO" --send_logs_level=trace \
    --partial_result_on_first_cancel=1 --query_id="$QUERY_ID" \
    --query "SELECT count() FROM numbers(1000000000)
             SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                      max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
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

# One Ctrl+C only asks for the partial result, but it must already unblock the client.
kill -SIGINT "$CLIENT" 2>/dev/null

for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still running after the first Ctrl+C"
    kill -9 "$CLIENT" 2>/dev/null
    exit 0
fi

echo "OK: client terminated after the first Ctrl+C"

# The point of the test: an unusable log sink must not swallow the partial result. Without this,
# the interrupted `open()` escapes the log packet handler as a cancellation before the stage-one
# `Cancel` is sent, and the query returns nothing at all.
if grep -qE '^[0-9]+$' "$CLIENT_OUT"
then
    echo "OK: the partial result was returned"
else
    echo "FAIL: no partial result was returned"
    echo "--- client stdout ---"
    cat "$CLIENT_OUT"
    echo "--- client stderr ---"
    cat "$CLIENT_ERR"
fi
