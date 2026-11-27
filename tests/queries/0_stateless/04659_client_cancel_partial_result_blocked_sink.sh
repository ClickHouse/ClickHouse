#!/usr/bin/env bash
# Tags: no-fasttest

# With `partial_result_on_first_cancel` the query is stopped only by the second Ctrl+C: the first
# one has to reach the server as a stage-one `Cancel`, so that the partial result is delivered.
# That can only happen if the first signal already breaks the client out of a write to an output
# sink that is not draining - otherwise the client stays stuck in that write, never returns to
# `receiveResult`, and the setting silently does nothing on exactly the paths that need it.
# 04616_client_cancel_blocked_outfile exercises the same blocked sink with the default
# single-signal cancellation.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_blocked.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_partial_result_blocked.err"

CLIENT=""
HOLDER=""
DRAINER=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    [ -n "$HOLDER" ] && kill "$HOLDER" 2>/dev/null
    [ -n "$DRAINER" ] && kill "$DRAINER" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_ERR"
}
trap cleanup EXIT

# The `INTO OUTFILE` sink is a FIFO with a reader that opens the pipe but never consumes data, so
# the client fills the pipe buffer and then blocks in write(). `APPEND` makes the client open the
# pre-created FIFO directly (a plain `INTO OUTFILE` refuses an existing file, and `TRUNCATE` would
# write to a temporary regular file instead).
mkfifo "$FIFO"
sleep 1000 < "$FIFO" &
HOLDER=$!

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_partial_result_blocked"

$CLICKHOUSE_CLIENT --partial_result_on_first_cancel=1 --query_id="$QUERY_ID" \
    --query "SELECT number, repeat('x', 100) FROM numbers(1000000000)
             INTO OUTFILE '$FIFO' APPEND FORMAT TabSeparated
             SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                      max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > /dev/null 2> "$CLIENT_ERR" &
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
    exit 0
fi

# One Ctrl+C only asks for the partial result, but it must already unblock the client: the stage-one
# `Cancel` reaches the server, the query finishes and the client returns to the shell without a
# second signal. Once cancellation is requested, let the FIFO drain so the server's partial result
# has a deliverable destination; otherwise the test would correctly leave the client waiting for
# output that the deliberately non-reading sink can never accept.
kill -SIGINT "$CLIENT" 2>/dev/null
kill "$HOLDER" 2>/dev/null
wait "$HOLDER" 2>/dev/null
HOLDER=""
cat "$FIFO" > /dev/null &
DRAINER=$!

for _ in {0..50}
do
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.2
done

if kill -0 "$CLIENT" 2>/dev/null
then
    echo "FAIL: client is still running after the first Ctrl+C"
    kill -9 "$CLIENT" 2>/dev/null
else
    echo "OK: client terminated after the first Ctrl+C"
fi
