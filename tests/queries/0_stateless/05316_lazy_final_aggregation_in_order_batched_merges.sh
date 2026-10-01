#!/usr/bin/env bash

# Lazy FINAL with optimize_aggregation_in_order: the deduplicating aggregation must merge groups in
# blocks bounded by aggregation_in_order_max_block_bytes, not one group at a time (issue #114579).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_lazy_final_in_order;
    CREATE TABLE t_lazy_final_in_order (k UInt64, version UInt64, is_deleted UInt8, v UInt64)
    ENGINE = ReplacingMergeTree(version, is_deleted) ORDER BY k;
    SYSTEM STOP MERGES t_lazy_final_in_order;
    INSERT INTO t_lazy_final_in_order SELECT number, 1, 0, number FROM numbers(20000);
    INSERT INTO t_lazy_final_in_order SELECT number, 2, if(number % 10 = 0, 1, 0), number * 2 FROM numbers(10000, 15000);
"

query="SELECT count(), sum(v) FROM t_lazy_final_in_order FINAL WHERE k % 7 != 6"
settings="max_threads = 4, max_block_size = 8192, query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000,
    min_filtered_ratio_for_lazy_final = 0, optimize_aggregation_in_order = 1, aggregation_in_order_max_block_bytes = 50000000"

# Same result as the regular FINAL read.
$CLICKHOUSE_CLIENT -q "$query SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT -q "$query SETTINGS $settings"

log=$($CLICKHOUSE_CLIENT --send_logs_level=trace -q "$query SETTINGS $settings FORMAT Null" 2>&1)
echo "$log" | grep -o 'Lazy FINAL enabled' | head -1
merges=$(echo "$log" | grep -c 'Merging partially aggregated blocks')
# One merge per group would be ~20000 merges here.
[ "$merges" -ge 1 ] && [ "$merges" -le 100 ] && echo "batched merges: OK" || echo "batched merges: $merges"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_final_in_order"
