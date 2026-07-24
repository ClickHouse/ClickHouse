#!/usr/bin/env bash
# Tags: no-old-analyzer, no-random-settings
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-random-settings: the test pins a precise plan (a partitioned window sort that fans out across
# streams) and compares it against the non-distributed result; randomized settings can change whether
# make_distributed_plan distributes the query, which would invalidate the comparison.

# A PARTITION BY window under make_distributed_plan=1 must produce the same result as the
# non-distributed plan. Regression test for a bug where a task's partitioned sort fans out to several
# streams (SortingStep::fullSort keeps one stream per partition when max_threads > 1) and GatherSend
# collapsed them with ResizeProcessor, which does not preserve order, so the window on the receiving
# side was computed over the wrong row sequence.
#
# The window output rows are compared directly (not via an aggregate over the distributed query):
# make_distributed_plan refuses to distribute an aggregation while max_rows_to_group_by is set, and the
# stateless test profile sets it, so an aggregate checksum would never run under make_distributed_plan.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_window_partition_order"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_window_partition_order (a UInt32, v UInt32)
    ENGINE = MergeTree ORDER BY (a, v) SETTINGS index_granularity = 1024"

# Many partitions and several parts so the distributed task's sort fans out to multiple streams.
for off in 0 1000000 2000000; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO t_window_partition_order
        SELECT number % 50, number + ${off} FROM numbers(200000)"
done

WINDOW_QUERY="
    SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s
    FROM t_window_partition_order ORDER BY a, v"

reference=$($CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY} SETTINGS make_distributed_plan = 0" | md5sum)

distributed=$($CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY}" \
    --make_distributed_plan 1 --optimize_read_in_order 0 --enable_parallel_replicas 0 \
    --distributed_plan_execute_locally 1 --distributed_plan_max_rows_to_broadcast 0 \
    --enable_join_runtime_filters 0 --distributed_plan_default_shuffle_join_bucket_count 8 \
    --distributed_plan_default_reader_bucket_count 8 | md5sum)

if [ "$reference" = "$distributed" ]; then
    echo "OK"
else
    echo "FAIL: distributed=${distributed} reference=${reference}"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_window_partition_order"
