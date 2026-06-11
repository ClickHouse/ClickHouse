#!/usr/bin/env bash
# Tags: no-async-insert, no-fasttest, no-random-detach
# no-fasttest: Too slow for fast test (~14s), covered by regular stateless runs.
# no-async-insert: Test expects new part for each time interval
# no-random-detach: a randomized DETACH/ATTACH before the streaming INSERT disrupts the
#                   timing-sensitive partial flush.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_insert_timeout"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_insert_timeout (id UInt64, data String) ENGINE MergeTree ORDER BY id"

# Feed the rows through a FIFO instead of piping a subshell straight into the client.
# A subshell pipe (`{ ...; sleep; ... } | client`) is racy under CI load: the producer can
# write every row and exit before a slow client starts draining stdin, so the client never
# observes an idle gap, the block-wait timeout never fires, and all rows land in one part.
# Writing to a FIFO the client already holds open keeps each per-interval idle gap real on a
# live pipe, so the timeout-driven partial flush is deterministic.
fifo="${CLICKHOUSE_TMP}/03802_insert_stream_timeout.fifo"
rm -f "$fifo"
mkfifo "$fifo"

# Tag the INSERT with an explicit query_id so the part count below can be scoped to exactly
# this INSERT. With async_insert disabled (no-async-insert tag) the parts are written in the
# INSERT's own context, so part_log.query_id equals this id.
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

# Open the write end. This blocks until the client opens the read end, synchronizing start.
# Let bash pick the descriptor (stream_fd) instead of hardcoding fd 3: shell_config.sh routes
# bash xtrace to fd 3 under CI (BASH_XTRACEFD), so writing the rows to fd 3 would interleave
# trace output into the INSERT stream and break parsing.
exec {stream_fd}>"$fifo"

for iteration in 1 2; do
    for i in $(seq 1 40); do
        echo "{\"id\":$(( (iteration*100) + i )),\"data\":\"batch_${iteration}\"}" >&${stream_fd}
    done

    sleep 6

    echo "{\"id\":$(( (iteration*100) + 99 )),\"data\":\"trigger_${iteration}\"}" >&${stream_fd}
done

# Close the write end so the client finishes the INSERT.
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
