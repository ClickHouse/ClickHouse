#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# A PARTITION BY window under make_distributed_plan=1 must produce the same result as the
# non-distributed plan. Regression test for a bug where a task's partitioned sort fans out to several
# streams (SortingStep::fullSort keeps one stream per partition when max_threads > 1) and GatherSend
# collapsed them with ResizeProcessor, which does not preserve order, so the window on the receiving
# side was computed over the wrong row sequence.

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
    SELECT sum(cityHash64(a, v, s))
    FROM (SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v) AS s FROM t_window_partition_order)"

reference=$($CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY} SETTINGS make_distributed_plan = 0")

distributed=$($CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY}" \
    --make_distributed_plan 1 --enable_parallel_replicas 0 --distributed_plan_execute_locally 1 \
    --distributed_plan_max_rows_to_broadcast 0 --enable_join_runtime_filters 0 \
    --distributed_plan_default_shuffle_join_bucket_count 8 --distributed_plan_default_reader_bucket_count 8)

if [ "$reference" = "$distributed" ]; then
    echo "OK"
else
    echo "FAIL: distributed=${distributed} reference=${reference}"
fi

$CLICKHOUSE_CLIENT --query "DROP TABLE t_window_partition_order"
