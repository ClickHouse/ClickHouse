#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pretty formats squash consecutive blocks and write them from a background thread. When that write
# fails (here: the reader of the pipe has exited), the error has to reach the query. Otherwise the
# query never learns that nobody reads its output and keeps running - this one would run forever.

# `output_format_pretty_max_rows` is raised so that every block is written, and the query keeps
# writing into the broken pipe. `output_format_pretty_squash_consecutive_ms` is pinned because the
# background writer is only used when it is non-zero. `timeout` exits with 124 if the query hangs.
# The query has to fail with the original write error, `CANNOT_WRITE_TO_FILE_DESCRIPTOR` (exit code 75),
# not with `QUERY_WAS_CANCELLED` or anything else.
# The reading is throttled to keep each squashed table small. Otherwise the first table holds everything read
# before the background thread writes, which on a fast machine exceeds the memory limit or the timeout.
timeout 60 $CLICKHOUSE_LOCAL --max_threads=1 --max_execution_speed=1000000 --timeout_before_checking_execution_speed=0 \
    --output_format_pretty_squash_consecutive_ms=50 --output_format_pretty_max_rows=1000000000000 \
    --query "SELECT number FROM numbers(1e18) FORMAT PrettyCompact" 2>/dev/null | head -n 1 > /dev/null

code=${PIPESTATUS[0]}
if [ "$code" -eq 124 ]
then
    echo "The query did not stop after its output pipe was broken"
elif [ "$code" -ne 75 ]
then
    echo "Unexpected exit code $code"
else
    echo "OK"
fi

# The same, when the query produces all of its output at the beginning and then keeps reading without
# writing anything: `DISTINCT` finds all 100000 values in the first blocks. They are squashed into one
# table, larger than the pipe buffer, so its write fails in the background thread. There are no more
# `write` calls to notice the error, so the query has to be cancelled by the background thread itself.
timeout 60 $CLICKHOUSE_LOCAL --max_threads=1 --output_format_pretty_squash_consecutive_ms=50 --output_format_pretty_max_rows=1000000000000 \
    --query "SELECT DISTINCT number % 100000 AS x FROM numbers(1e18) FORMAT PrettyCompact" 2>/dev/null | head -n 1 > /dev/null

code=${PIPESTATUS[0]}
if [ "$code" -eq 124 ]
then
    echo "The query did not stop after its output pipe was broken, with no more output to write"
elif [ "$code" -ne 75 ]
then
    echo "Unexpected exit code $code"
else
    echo "OK"
fi

# The same with the remote `clickhouse-client`, which formats the result itself. The query in its
# process list is on the server, so the background thread has nothing to cancel there: the error is
# reported by the client while it waits for packets from the server instead. `interactive_delay` is
# raised so that the server sends progress packets rarely: the client has to notice the error while it
# polls the connection, not only when it processes a packet. It is not raised further, because the
# server also checks for the `Cancel` packet only that often. The client has to cancel the query on the
# server: the server would never notice the client has gone away, because it has nothing more to send.
query_id="05238_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
timeout 60 $CLICKHOUSE_CLIENT --query_id="$query_id" --max_threads=1 --interactive_delay=2000000 --output_format_pretty_squash_consecutive_ms=50 --output_format_pretty_max_rows=1000000000000 --max_rows_to_read=0 \
    --query "SELECT DISTINCT number % 100000 AS x FROM numbers(1e18) FORMAT PrettyCompact" 2>/dev/null | head -n 1 > /dev/null

code=${PIPESTATUS[0]}
if [ "$code" -eq 124 ]
then
    echo "The remote client did not stop after its output pipe was broken, with no more output to write"
elif [ "$code" -ne 75 ]
then
    echo "Unexpected exit code $code"
else
    echo "OK"
fi

for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" -eq 0 ] && break
    sleep 0.1
done

if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" -ne 0 ]
then
    echo "The query kept running on the server after the remote client stopped"
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' SYNC" > /dev/null
else
    echo "OK"
fi
