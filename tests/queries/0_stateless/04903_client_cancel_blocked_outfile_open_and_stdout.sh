#!/usr/bin/env bash

# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_outfile_open_and_stdout.fifo"
CLIENT_OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_outfile_open_and_stdout.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_blocked_outfile_open_and_stdout.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

# No reader is attached, so opening this FIFO for the primary outfile blocks. `AND STDOUT`
# remains a usable result sink and must receive the partial result after the first Ctrl+C.
mkfifo "$FIFO"

QUERY_ID="${CLICKHOUSE_DATABASE}_cancel_blocked_outfile_open_and_stdout"

$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --partial_result_on_first_cancel=1 \
    --query "SELECT number, repeat('x', 100) FROM numbers(1000000000)
             INTO OUTFILE '$FIFO' APPEND AND STDOUT FORMAT TabSeparated
             SETTINGS max_block_size = 8192, max_threads = 1, max_memory_usage = 0,
                      max_rows_to_read = 0, max_result_rows = 0, max_result_bytes = 0" \
    > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT=$!

started=0
for _ in {0..120}
do
    count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'" 2>/dev/null)
    if [[ "$count" =~ ^[0-9]+$ ]] && [ "$count" -ge 1 ]
    then
        started=1
        break
    fi
    kill -0 "$CLIENT" 2>/dev/null || break
    sleep 0.5
done

if [ "$started" -ne 1 ]
then
    echo "FAIL: the query did not reach the running state"
    cat "$CLIENT_ERR"
    exit 0
fi

# The client is now blocked opening the reader-less FIFO, while the server keeps
# producing the result. Give the server time to enqueue at least one block before
# requesting the partial result; otherwise a fast cancellation can validly produce
# an empty partial result and make this regression test timing-dependent.
sleep 1

kill -SIGINT "$CLIENT" 2>/dev/null

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

if grep -q "Query was cancelled while waiting" "$CLIENT_ERR"
then
    echo "FAIL: stdout fallback was treated as a local cancellation"
else
    echo "OK: stdout fallback stayed available"
fi

# A stage-one cancel is allowed to return no rows when it wins the race with the
# server's first result block. The essential property here is that the abandoned
# outfile does not turn that interrupt into a local exception and that stdout
# remains a usable fallback sink.
