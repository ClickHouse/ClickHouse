#!/usr/bin/env bash
# Tags: long, no-fasttest

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
# We force ONE long consume() call: all rows in a single input chunk (large read-block settings, no
# concurrent-read split) with max_block_size / aggregation_in_order_max_block_bytes large enough that consume()
# does not return early. We KILL the query once every row is read, so it is inside that single consume() loop
# with all keys still to aggregate. WITH the fix KILL SYNC returns in a fraction of a second; WITHOUT it it
# blocks for the several seconds the loop needs to finish. We assert the KILL completes quickly.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_in_order_cancel"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_agg_in_order_cancel (c1 UInt64, c2 String, c3 UInt64, c4 String, c5 UInt64)
    ENGINE = MergeTree ORDER BY (c2, c3, c4, c5) SETTINGS index_granularity = 8192"

# Every row is a distinct group on the full sort-key prefix, so the key-interval loop runs once per row.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_agg_in_order_cancel
    SELECT number, toString(number), number, toString(number), number
    FROM numbers(10000000) SETTINGS max_insert_threads = 1, max_block_size = 2000000"
# One part so the reader delivers a single, non-split stream.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_agg_in_order_cancel FINAL"

query_id="04512_agg_in_order_cancel_${CLICKHOUSE_DATABASE}"

# optimize_aggregation_in_order + one big input chunk (large read block, no concurrent-read split) + large
# max_block_size / aggregation_in_order_max_block_bytes => one consume() call aggregates all 10M distinct keys.
$CLICKHOUSE_CLIENT --query_id "$query_id" --query "
    SELECT c1, c2, c3, c4, c5, count()
    FROM t_agg_in_order_cancel
    GROUP BY c1, c2, c3, c4, c5
    FORMAT Null
    SETTINGS optimize_aggregation_in_order = 1, max_threads = 1,
             max_block_size = 200000000, aggregation_in_order_max_block_bytes = '100G',
             preferred_block_size_bytes = '100G',
             merge_tree_min_rows_for_concurrent_read = 1000000000,
             read_in_order_two_level_merge_threshold = 1, max_memory_usage = 0" &>/dev/null &

wait_for_query_to_start "$query_id"

# Wait until every row is read: the query is now inside the single long consume() aggregation loop with all 10M
# keys still to aggregate. The query must still be running when we KILL it -- if it disappears first (e.g. it
# somehow completed before being cancelled) the test would not exercise cancellation, so that is a failure, not
# a pass.
while [[ $($CLICKHOUSE_CLIENT --query "SELECT read_rows >= 10000000 FROM system.processes WHERE query_id = '$query_id'") != 1 ]]; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'") == 0 ]]; then
        echo "query exited before cancellation"
        wait
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_in_order_cancel"
        exit 0
    fi
    sleep 0.1
done

# KILL SYNC waits until the query is actually cancelled and returns one row per matched query with its final
# kill_status. WITH the fix this returns in a fraction of a second (the per-interval isCancelled() check);
# WITHOUT it it blocks for the several seconds the single consume() needs to finish (longer still on slower CI
# builds). timeout 6 sits between the two: it always elapses well before the unfixed blocking time and long
# after the fixed case. We print the kill_status ("finished") only if the KILL completed in time AND actually
# matched and cancelled our still-running query; a missing fix (KILL times out) or a no-match KILL both fail
# to print "finished", so the reference will not match.
kill_status=$(timeout 6 $CLICKHOUSE_CLIENT --query \
    "KILL QUERY WHERE query_id = '$query_id' SYNC" 2>/dev/null | cut -f1)
[[ "$kill_status" == "finished" ]] && echo "cancelled" || echo "not cancelled in time"

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_in_order_cancel"
