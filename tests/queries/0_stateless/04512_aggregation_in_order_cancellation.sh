#!/usr/bin/env bash
# Tags: long, no-fasttest, no-flaky-check
# no-flaky-check: the flaky check reruns a changed test against the same, already fixed binary, so
# repeating this one there cannot show the cancellation regression it exists to catch. Every normal
# job runs it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# AggregatingInOrderTransform::consume() splits one input chunk into runs of equal keys, and checks cancellation
# once per key interval so a cancelled query stops there instead of aggregating the whole chunk.
#
# The settings below force ONE long consume() over all rows, so a KILL issued after the read leaves most of the
# keys unaggregated when the checkpoint works and none of them when it does not.

table="t_agg_in_order_cancel_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $table (k UInt64)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192"

# Every row is a distinct group on the sort key, so the key-interval loop runs once per row, and one squashed
# INSERT writes a single part so the reader delivers one non-split stream. The row count keeps aggregating them far
# more expensive than reading them, which is what the assertion below compares.
# max_rows_to_read = 0: the default stateless profile sets max_rows_to_read = 20000000, which numbers() enforces up
# front, so without this the INSERT throws TOO_MANY_ROWS before it runs.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO $table
    SELECT number FROM numbers(40000000)
    SETTINGS max_insert_threads = 1, max_block_size = 40000000,
             min_insert_block_size_rows = 40000000, min_insert_block_size_bytes = 0,
             max_memory_usage = 4000000000, max_rows_to_read = 0"

query_id="04512_agg_in_order_cancel_${CLICKHOUSE_DATABASE}"
scan_id="04512_agg_in_order_scan_${CLICKHOUSE_DATABASE}_$$"

# Read the same rows once without aggregating, to price the read on this build. The oracle below is a multiple of
# this, so sanitizer instrumentation, compression and architecture scale the threshold instead of invalidating it.
$CLICKHOUSE_CLIENT --query_id "$scan_id" --query "
    SELECT k FROM $table FORMAT Null
    SETTINGS max_threads = 1, max_block_size = 200000000, preferred_block_size_bytes = '100G',
             merge_tree_min_rows_for_concurrent_read = 1000000000, max_rows_to_read = 0,
             enable_parallel_replicas = 0, log_profile_events = 1"

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
             enable_parallel_replicas = 0, log_profile_events = 1" &>/dev/null &

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

# KILL SYNC returns one row per matched query with its final kill_status. The outer timeout only keeps the test
# bounded if the KILL never returns at all; the assertion on how much aggregation the cancelled query still did is
# below.
kill_id="04512_agg_in_order_kill_${CLICKHOUSE_DATABASE}_$$"
kill_status=$(timeout 120 $CLICKHOUSE_CLIENT --query_id "$kill_id" --query \
    "KILL QUERY WHERE query_id = '$query_id' SYNC" 2>/dev/null | cut -f1)

wait

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# Returning at the next key interval leaves the rest of the loop unaggregated, while ignoring cancellation charges
# the whole loop, so CPU time counts the work the checkpoint avoids and no load can inflate it. mapContains keeps a
# row whose counters were not logged from reading as zero, which would pass; it must be a missing measurement.
function query_cpu_us()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT if(mapContains(ProfileEvents, 'UserTimeMicroseconds'),
                  toString(ProfileEvents['UserTimeMicroseconds']), '')
        FROM system.query_log
        WHERE event_date >= yesterday() AND type != 'QueryStart'
          AND current_database = currentDatabase() AND query_id = '$1'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

cancel_cpu_us=$(query_cpu_us "$query_id")
scan_cpu_us=$(query_cpu_us "$scan_id")

# Measured on this fixture: aggregating up to cancellation costs under 2.4 times the plain read, while running the
# loop to completion costs about 40 times it, so a multiple of the read separates them on any build.
kill_verdict=no-measurement
if [[ -n "$cancel_cpu_us" && -n "$scan_cpu_us" && "$scan_cpu_us" != "0" ]]; then
    kill_verdict=$($CLICKHOUSE_CLIENT --query "
        SELECT if($cancel_cpu_us < 8 * $scan_cpu_us, 'quick', 'slow')")
fi

# "finished" proves the KILL matched and cancelled our still-running query, "quick" that it did not wait for
# the aggregation loop.
if [[ "$kill_status" == "finished" && "$kill_verdict" == "quick" ]]; then
    echo "cancelled"
else
    echo "not cancelled in time"
    echo "kill_status=$kill_status kill_verdict=$kill_verdict cancel_cpu_us=$cancel_cpu_us scan_cpu_us=$scan_cpu_us"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table"
