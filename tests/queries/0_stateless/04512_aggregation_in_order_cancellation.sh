#!/usr/bin/env bash
# Tags: long, no-fasttest, no-flaky-check
# no-flaky-check: this asserts cancellation latency (a cancelled query must stop before a
# wall-clock timeout), so repeated concurrent reruns under sanitizers only race on wall clock.
# Same as the sibling KILL cancellation tests 03984_kill_query_with_fill and
# 04028_kill_query_expression_cancelled.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# AggregatingInOrderTransform::consume() splits one input chunk into runs of equal keys in a loop. Before the
# fix, cancellation was only checked between work() calls, so a single consume() over a chunk with many distinct
# keys ran to completion ignoring is_cancelled. A cancelled query (KILL QUERY / max_execution_time) then kept
# aggregating and the connection thread blocked in PullingAsyncPipelineExecutor::cancel() -> join() waiting for
# that loop, which the server-side AST fuzzer repeatedly hit as "Hung check failed, possible deadlock found".
# The loop now checks isCancelled() once per key interval and returns promptly.
#
# We force ONE long consume() call: all rows in a single part and a single input chunk (large read-block
# settings, no concurrent-read split) with max_block_size / aggregation_in_order_max_block_bytes large enough
# that consume() does not return early. A single UInt64 key column keeps the per-key aggregation state tiny
# (peak query memory well under a GiB), so the parallel flaky check can run several copies at once without
# memory pressure -- an earlier wide fixture starved under contention and hit the per-run timeout. The single
# part is built by one squashed INSERT (min_insert_block_size_rows covers all rows) so no OPTIMIZE FINAL merge
# is needed. We KILL the query once every row is read, so it is inside that single consume() loop with all keys
# still to aggregate. WITH the fix KILL SYNC returns in a fraction of a second; WITHOUT it it blocks for the
# several seconds the loop needs. We assert the KILL completes quickly.

table="t_agg_in_order_cancel_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $table (k UInt64)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192"

# Every row is a distinct group on the sort key, so the key-interval loop runs once per row. One squashed INSERT
# (min_insert_block_size_rows and max_block_size both >= row count) writes a single part, so the reader delivers
# a single, non-split stream without needing OPTIMIZE FINAL. A single UInt64 column keeps this cheap and
# low-memory (peak query memory well under a GiB). 40M rows make the uncancelled consume() loop take several
# seconds even on a fast release build, so the KILL-timeout assertion below stays directional across build types.
# max_rows_to_read = 0: the default stateless profile sets max_rows_to_read = 20000000, which numbers()
# enforces up front, so without this the INSERT throws TOO_MANY_ROWS on numbers(40000000) before it runs.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO $table
    SELECT number FROM numbers(40000000)
    SETTINGS max_insert_threads = 1, max_block_size = 40000000,
             min_insert_block_size_rows = 40000000, min_insert_block_size_bytes = 0,
             max_memory_usage = 4000000000, max_rows_to_read = 0"

query_id="04512_agg_in_order_cancel_${CLICKHOUSE_DATABASE}"

# optimize_aggregation_in_order + one big input chunk (large read block, no concurrent-read split) + large
# max_block_size / aggregation_in_order_max_block_bytes => one consume() call aggregates all 40M distinct keys.
# enable_parallel_replicas = 0: with parallel replicas the query runs on the replica cluster and the initiator
# query_id does not appear in system.processes, so wait_for_query_to_start below never sees it and times out.
# This test targets the single-node AggregatingInOrderTransform path, so pin it off (CI randomizes it on).
# max_rows_to_read = 0: overrides the default stateless profile's max_rows_to_read = 20000000, which would
# otherwise abort this 40M-row read with TOO_MANY_ROWS before the cancellation path is reached.
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT k, count()
    FROM $table
    GROUP BY k
    FORMAT Null
    SETTINGS optimize_aggregation_in_order = 1, max_threads = 1,
             max_block_size = 200000000, aggregation_in_order_max_block_bytes = '100G',
             preferred_block_size_bytes = '100G',
             merge_tree_min_rows_for_concurrent_read = 1000000000,
             read_in_order_two_level_merge_threshold = 1, max_memory_usage = 0,
             max_rows_to_read = 0,
             enable_parallel_replicas = 0" &>/dev/null &

wait_for_query_to_start "$query_id"

# Wait until every row is read: the query is now inside the single long consume() aggregation loop with all 40M
# keys still to aggregate. The query must still be running when we KILL it -- if it disappears first (e.g. it
# somehow completed before being cancelled) the test would not exercise cancellation, so that is a failure, not
# a pass.
while [[ $($CLICKHOUSE_CLIENT --query "SELECT read_rows >= 40000000 FROM system.processes WHERE query_id = '$query_id'") != 1 ]]; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'") == 0 ]]; then
        echo "query exited before cancellation"
        wait
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table"
        exit 0
    fi
    sleep 0.1
done

# KILL SYNC waits until the query is actually cancelled and returns one row per matched query with its final
# kill_status. WITH the fix this returns in a fraction of a second (the per-interval isCancelled() check returns
# after one interval, regardless of size); WITHOUT it it blocks for the several seconds the single consume() over
# 40M keys needs to finish (measured ~4-6s release, ~11s debug, ~28-48s under sanitizers). timeout 3 sits between
# the two: it always elapses well before the unfixed blocking time and long after the fixed case. We print the
# kill_status ("finished") only if the KILL completed in time AND actually matched and cancelled our still-running
# query; a missing fix (KILL times out) or a no-match KILL both fail to print "finished", so the reference will
# not match.
kill_status=$(timeout 3 $CLICKHOUSE_CLIENT --query \
    "KILL QUERY WHERE query_id = '$query_id' SYNC" 2>/dev/null | cut -f1)
[[ "$kill_status" == "finished" ]] && echo "cancelled" || echo "not cancelled in time"

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table"
