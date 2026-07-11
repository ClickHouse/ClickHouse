#!/usr/bin/env bash
# Tags: long, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# AggregatingInOrderTransform::consume() splits one input chunk into runs of equal keys in a loop; each
# iteration does an upper_bound over the remaining rows, so a chunk with many distinct keys makes a single
# consume() call run for a long time (O(distinct_keys) iterations). Query time and cancellation limits are
# only checked between pipeline steps (between work() calls), so before the in-loop check this test covers,
# such a consume() ignored max_execution_time / KILL QUERY: the connection thread then blocked in
# PullingAsyncPipelineExecutor::cancel() -> join() waiting for the running loop, which the server-side AST
# fuzzer repeatedly tripped as "Hung check failed, possible deadlock found". The loop now checks
# isCancelled() once per key interval and stops promptly.
#
# The signal is latency: WITH the fix the query stops within max_execution_time and prints TIMEOUT_EXCEEDED;
# WITHOUT it the loop runs for tens of seconds, the `timeout` wrapper kills the client and the output
# differs from the reference.

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_in_order_cancel"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_agg_in_order_cancel (c1 UInt64, c2 String, c3 UInt64, c4 String, c5 UInt64)
    ENGINE = MergeTree ORDER BY (c2, c3, c4, c5) SETTINGS index_granularity = 8192"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_agg_in_order_cancel
    SELECT number, toString(number % 1000), number, toString(number % 777), number
    FROM numbers(5000000) SETTINGS max_insert_threads = 1, max_block_size = 1000000"

# GROUP BY on the full sort key prefix triggers optimize_aggregation_in_order; every row is a distinct group,
# so one consume() call iterates over the whole chunk. max_execution_time = 1 must interrupt that loop.
timeout 40 $CLICKHOUSE_CLIENT --max_execution_time 1 --timeout_overflow_mode throw --query "
    SELECT c1, c2, c3, c4, c5, ROW_NUMBER() OVER (PARTITION BY c2, c3, c4 ORDER BY c5 DESC) AS rn
    FROM t_agg_in_order_cancel
    GROUP BY c1, c2, c3, c4, c5
    ORDER BY c2, c4, c5
    FORMAT Null
    SETTINGS optimize_aggregation_in_order = 1, max_threads = 1,
             read_in_order_two_level_merge_threshold = 1, max_memory_usage = 0" 2>&1 \
    | grep -o -m1 "TIMEOUT_EXCEEDED" || echo "no timeout"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_agg_in_order_cancel"
