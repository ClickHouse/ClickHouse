#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Test that LazyFinalKeyAnalysisTransform logs the correct decision
# for each case: set truncated, filtered ratio too low, optimization enabled.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_log;
    CREATE TABLE t_lazy_log (key UInt64, version UInt64, category UInt8, payload String)
    ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 64;
    SYSTEM STOP MERGES t_lazy_log;

    -- Concentrated: category=1 only for keys 0..49
    INSERT INTO t_lazy_log SELECT number, 1, if(number < 50, 1, 0), 'x' FROM numbers(1000);
    INSERT INTO t_lazy_log SELECT number + 1000, 1, 0, 'x' FROM numbers(1000);
    INSERT INTO t_lazy_log SELECT number + 2000, 1, 0, 'x' FROM numbers(1000);

    -- Scattered: category=2 every 10th row across all key ranges
    INSERT INTO t_lazy_log SELECT number * 10, 2, 2, 'x' FROM numbers(300);
"

## Case 1: Set truncated (max_rows too small)
echo "=== Case 1: set truncated ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_log FINAL WHERE category = 1
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10
" --send_logs_level='trace' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Lazy FINAL' \
    | sed 's/.*Lazy FINAL/Lazy FINAL/' \
    | sed -E 's/=[0-9]+/=N/g' \
    | head -1

## Case 2: Filtered ratio too low (scattered filter, threshold 0.5)
echo "=== Case 2: filtered ratio too low ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_log FINAL WHERE category = 2
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5
" --send_logs_level='trace' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Lazy FINAL' \
    | sed 's/.*Lazy FINAL/Lazy FINAL/' \
    | sed -E 's/[0-9]+\.[0-9]+/N.NN/g; s/=[0-9]+/=N/g' \
    | head -1

## Case 3: Optimization enabled (concentrated filter, good pruning)
echo "=== Case 3: optimization enabled ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_log FINAL WHERE category = 1
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5
" --send_logs_level='trace' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Lazy FINAL' \
    | sed 's/.*Lazy FINAL/Lazy FINAL/' \
    | sed -E 's/[0-9]+\.[0-9]+/N.NN/g; s/=[0-9]+/=N/g' \
    | head -1

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_log"
