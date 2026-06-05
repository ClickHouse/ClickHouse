#!/usr/bin/env bash
# Tags: no-fasttest

# Pressing Ctrl+C in the client must terminate the output of a result set promptly,
# even when the client is blocked writing to a slow/stuck output sink.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_blocked_output"
FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_output.err"
rm -f "$FIFO"
mkfifo "$FIFO"

CLIENT=""
HOLDER=""
cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    [ -n "$HOLDER" ] && kill "$HOLDER" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_ERR"
}
trap cleanup EXIT

# A reader that opens the pipe but never consumes data: the client quickly fills the pipe
# buffer and then blocks inside write(). Without an interruptible write, a single Ctrl+C
# would only set the cancellation flag while the restarted write() keeps the client stuck.
sleep 1000 < "$FIFO" &
HOLDER=$!

# An effectively unbounded result so the server keeps the query running (blocked on the stuck
# sink). Small blocks and disabled limits keep the test cheap and immune to the randomized
# settings used by the flaky check, which could otherwise make the client error out early
# (e.g. a low max_memory_usage) instead of reaching the blocked state we want to exercise.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" \
    --query "SELECT number, repeat('x', 100) FROM numbers(1000000000)
             SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                      max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > "$FIFO" 2> "$CLIENT_ERR" &
CLIENT=$!

# Wait until the query is actually running (and thus blocked writing the result).
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
    exit 1
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
else
    echo "OK: client terminated after Ctrl+C"
fi
