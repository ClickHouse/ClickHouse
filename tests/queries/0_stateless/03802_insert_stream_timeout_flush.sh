#!/usr/bin/env bash
# Tags: no-async-insert, no-fasttest
# no-fasttest: Too slow for fast test (~14s), covered by regular stateless runs.
# no-async-insert: Test expects new part for each time interval

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"

# Feed the rows through a FIFO the client reads as stdin. The synchronisation that makes the
# timeout flush deterministic is OS pipe back-pressure: a fresh pipe holds 64 KiB, so a first
# batch larger than that cannot be fully written until clickhouse-client is up and draining
# stdin. The producer's write() therefore blocks until the client is actually consuming rows,
# which is a stronger guarantee than any server-side readiness probe (a query is registered in
# system.processes before the client reaches its stdin poll loop, so polling it can race a slow
# client and let both batches be buffered as one block).
fifo="${CLICKHOUSE_TMP}/03802_insert_stream_timeout.fifo"
rm -f "$fifo"
mkfifo "$fifo"

# A wide payload so the first batch alone exceeds the 64 KiB pipe buffer. With 100 rows of ~1 KiB
# each the batch is ~100 KiB, which both forces back-pressure and stays below max_insert_block_size
# so it is parsed as a single block (and parses fast, so the block-wait timeout cannot fire and
# split the batch before the idle gap below).
payload=$(printf 'x%.0s' $(seq 1 1024))
rows_per_batch=100

# Explicit query_id so the part count below can be scoped to exactly this INSERT. part_log is
# append-only across DROP TABLE, so a rerun reusing the table name would otherwise see earlier
# rows; with async_insert disabled (no-async-insert tag) the parts are written in the INSERT's
# own context, so part_log.query_id equals this id.
insert_query_id="03802_${CLICKHOUSE_DATABASE}_$$"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_timeout FORMAT JSONEachRow" \
    --query_id="$insert_query_id" \
    --max_insert_block_size=1000 \
    --input_format_connection_handling=1 \
    --input_format_max_block_wait_ms=2000 \
    --min_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=0 \
    < "$fifo" &
client_pid=$!

# Let bash pick the write descriptor instead of hardcoding fd 3: shell_config.sh routes bash
# xtrace to fd 3 under CI (BASH_XTRACEFD), so writing rows to fd 3 would interleave trace output
# into the INSERT stream and break parsing.
exec {stream_fd}>"$fifo"

emit_batch() {
    local base=$1
    for i in $(seq 1 "$rows_per_batch"); do
        echo "{\"id\":$(( base + i )),\"data\":\"${payload}\"}" >&${stream_fd}
    done
}

# Batch 1 is larger than the pipe buffer, so this write blocks until the client has drained the
# pipe, i.e. is past startup and actively reading stdin.
emit_batch 100

# The client is now reading, so this idle gap (> input_format_max_block_wait_ms) elapses inside
# its stdin poll() and fires the block-wait timeout, closing batch 1 into its own block.
sleep 3
emit_batch 200

# Close the write end so the client flushes the final block and finishes the INSERT.
exec {stream_fd}>&-
wait "$client_pid"
rm -f "$fifo"

record_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_insert_timeout")
echo "Total records inserted: ${record_count}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS part_log"

# Count parts this INSERT created. query_id scopes the count to this exact INSERT, so part_log
# rows left by an earlier run reusing the table name are excluded. merge_reason = 'NotAMerge'
# drops parts from a background merge. No query_log join, so parallel replicas cannot break it.
parts_count=$(${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.part_log
WHERE event_type = 'NewPart'
  AND merge_reason = 'NotAMerge'
  AND query_id = '$insert_query_id'")

echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"
