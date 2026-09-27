#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# A PARTITION BY window under make_distributed_plan=1 must produce the same result as the
# non-distributed plan, in the same row order. Every setting the comparison depends on is pinned
# on the client calls below; other settings stay randomized. The plan check catches the case when
# make_distributed_plan declines the query under some settings combination. Regression test for a bug where a task's partitioned
# sort fans out to several streams (SortingStep::fullSort keeps one stream per partition when
# max_threads > 1) and GatherSend collapsed them with ResizeProcessor, which does not preserve order,
# so the window on the receiving side was computed over the wrong row sequence.
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

# Without this check the comparison below passes vacuously if make_distributed_plan ever declines
# the query (both runs would be local). A substring check, not a plan snapshot, to stay robust against
# unrelated plan changes.
plan=$($CLICKHOUSE_CLIENT --max_threads 8 --query "EXPLAIN ${WINDOW_QUERY}" \
    --make_distributed_plan 1 --optimize_read_in_order 0 --enable_parallel_replicas 0 \
    --distributed_plan_execute_locally 1 --distributed_plan_max_rows_to_broadcast 0 \
    --enable_join_runtime_filters 0 --distributed_plan_default_shuffle_join_bucket_count 8 \
    --distributed_plan_default_reader_bucket_count 8)
if ! echo "${plan}" | grep -q "GatherExchange"; then
    echo "FAIL: the query did not produce a distributed plan:"
    echo "${plan}"
fi

reference_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local.tsv"
distributed_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_distributed.tsv"

$CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY} SETTINGS make_distributed_plan = 0" > "$reference_file"

$CLICKHOUSE_CLIENT --max_threads 8 --query "${WINDOW_QUERY}" \
    --make_distributed_plan 1 --optimize_read_in_order 0 --enable_parallel_replicas 0 \
    --distributed_plan_execute_locally 1 --distributed_plan_max_rows_to_broadcast 0 \
    --enable_join_runtime_filters 0 --distributed_plan_default_shuffle_join_bucket_count 8 \
    --distributed_plan_default_reader_bucket_count 8 > "$distributed_file"

if cmp -s "$reference_file" "$distributed_file"; then
    echo "OK"
else
    # The full outputs have 600k rows; print the row counts and the start of the diff, enough to
    # see whether the content or only the order changed, and where the first divergence is.
    echo "FAIL: the distributed result differs from the non-distributed one"
    echo "row counts: local=$(wc -l < "$reference_file") distributed=$(wc -l < "$distributed_file")"
    diff "$reference_file" "$distributed_file" | head -20
fi

rm -f "$reference_file" "$distributed_file"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_window_partition_order"
