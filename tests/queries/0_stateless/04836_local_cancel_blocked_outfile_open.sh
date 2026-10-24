#!/usr/bin/env bash
# Tags: no-fasttest

# The `clickhouse-local` twin of 04654_client_cancel_blocked_outfile_open: a single Ctrl+C must
# terminate the query promptly even when the client is blocked in `open()` of the primary
# `INTO OUTFILE` sink (a FIFO with no reader attached).
#
# For `LocalConnection` the query pipeline is created in `sendQuery` but starts lazily on the
# first pull, which happens only after the header block was delivered - and the client blocks
# opening the sink exactly on that header block, before anything was pulled. A cancellation
# arriving at that point used to be lost (`PullingAsyncPipelineExecutor::cancel` was a no-op
# before the first pull), so the post-cancel drain in `receiveResult` started the
# already-cancelled query and the client kept "draining" it to completion - for this query,
# practically forever - ignoring further Ctrl+C.
# See https://github.com/ClickHouse/ClickHouse/issues/22426

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_local_blocked_outfile_open.fifo"
CLIENT_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_local_blocked_outfile_open.err"

CLIENT=""

cleanup()
{
    [ -n "$CLIENT" ] && kill -9 "$CLIENT" 2>/dev/null
    wait 2>/dev/null
    rm -f "$FIFO" "$CLIENT_ERR"
}
trap cleanup EXIT

# No reader is ever attached to this FIFO, so the client stays inside the interruptible `open()`
# wait for the whole test. `APPEND` makes the client open the pre-created FIFO directly (a plain
# `INTO OUTFILE` refuses an existing file, and `TRUNCATE` would write to a temporary regular file
# instead).
mkfifo "$FIFO"

# The result set must be slow to produce: a cancellation that is only latched but not delivered
# to the executor would leave the post-cancel drain consuming the query as fast as it runs, and a
# cheap query would end that drain quickly enough to look like a prompt termination. `sleepEachRow`
# keeps the un-cancelled query running for hours, so only a real cancellation ends it promptly.
# The header block is delivered before the first row, so the client parks in the sink `open()`
# right away regardless of the sleeps.
$CLICKHOUSE_LOCAL \
    --query "SELECT sleepEachRow(0.1) FROM numbers(1000000)
             INTO OUTFILE '$FIFO' APPEND FORMAT TabSeparated
             SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 0,
                      max_threads = 1, max_rows_to_read = 0, max_result_rows = 0" \
    > /dev/null 2> "$CLIENT_ERR" &
CLIENT=$!

# There is no server to ask whether the query is running, and the blocked `open()` has no
# observable side effect, so give the client ample time to start the query and park in the wait.
# The assertion below holds in every interleaving: wherever the single SIGINT lands - before the
# open, inside it, or in the receive loop - the client must terminate promptly.
sleep 3

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

# The wait must be abandoned as a cancellation, not surfaced as a failure to open the file.
if grep -q "Cannot open file" "$CLIENT_ERR"
then
    echo "FAIL: reported as a failure to open the output file"
    cat "$CLIENT_ERR"
else
    echo "OK: reported as a cancellation"
fi
