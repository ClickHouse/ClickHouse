#!/usr/bin/env bash
# Tags: no-async-insert, no-fasttest
# no-fasttest: Too slow for fast test (~14s), covered by regular stateless runs.
# no-async-insert: Test expects new part for each time interval

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"

# Feed the rows through a FIFO the client already holds open, and gate the inter-batch idle gap
# on a real client-readiness handshake instead of a blind sleep that races client startup.
fifo="${CLICKHOUSE_TMP}/03802_insert_stream_timeout.fifo"
rm -f "$fifo"
mkfifo "$fifo"

# Tag the INSERT with an explicit query_id so the part count below can be scoped to exactly
# this INSERT, and so the readiness handshake can find it in system.processes. With async_insert
# disabled (no-async-insert tag) the parts are written in the INSERT's own context, so
# part_log.query_id equals this id.
insert_query_id="03802_${CLICKHOUSE_DATABASE}_$$"
rows_per_batch=41

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_insert_timeout FORMAT JSONEachRow" \
    --query_id="$insert_query_id" \
    --max_insert_block_size=1000 \
    --input_format_connection_handling=1 \
    --input_format_max_block_wait_ms=2000 \
    --min_insert_block_size_bytes=0 \
    --min_insert_block_size_rows=0 \
    < "$fifo" &
client_pid=$!

# Open the write end. Let bash pick the descriptor (stream_fd) instead of hardcoding fd 3:
# shell_config.sh routes bash xtrace to fd 3 under CI (BASH_XTRACEFD), so writing rows to fd 3
# would interleave trace output into the INSERT stream and break parsing.
exec {stream_fd}>"$fifo"

emit_batch() {
    local base=$1
    for i in $(seq 1 "$rows_per_batch"); do
        echo "{\"id\":$(( base + i )),\"data\":\"batch\"}" >&${stream_fd}
    done
}

# Batch 1, then wait for the INSERT to appear in system.processes. Opening the FIFO write end
# only proves the child shell opened the read end; the client still has to connect and reach its
# stdin poll loop afterwards. The query registers once the client has sent its first block, which
# proves it is now draining stdin, so a later idle gap really reaches its poll() instead of being
# pre-buffered ahead of a slow client. Wall-clock deadline so slow CI builds still catch it.
emit_batch 100
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    started=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$insert_query_id'")
    if (( started >= 1 )); then
        break
    fi
    sleep 0.2
done

# The client is confirmed reading, so this idle gap (> input_format_max_block_wait_ms) reaches
# its poll() and fires the block-wait timeout, closing batch 1 into its own block.
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
# rows left by an earlier run reusing the table name (part_log is append-only across DROP) are
# excluded. merge_reason = 'NotAMerge' drops parts from a background merge. No query_log join,
# so parallel replicas cannot break it either.
parts_count=$(${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.part_log
WHERE event_type = 'NewPart'
  AND merge_reason = 'NotAMerge'
  AND query_id = '$insert_query_id'")

echo "Number of parts created: ${parts_count}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"
