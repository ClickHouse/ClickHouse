#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-debug

# Test lazy FINAL optimization with all three filtering mechanisms:
# 1. Non-intersecting parts split (~20% of total marks)
# 2. PK index analysis via set (additional ~67% of PK-filtered marks pruned)
# 3. Non-PK predicate (status = 'active' filters ~50% of remaining rows)
#
# Data layout (ORDER BY key, index_granularity=128):
#   Parts 1,2: keys 0..3999 (overlapping → intersecting), ~32 marks each
#   Part 3: keys 4000..4999 (non-intersecting), ~8 marks
#   Part 4: keys 5000..5999 (non-intersecting), ~8 marks
#
# status='active' is concentrated in keys 0..999 for intersecting parts,
# so the set contains only keys 0..999.
#
# WHERE key >= 500 AND key < 5500 AND status = 'active':
#   PK alone: prunes keys 0..499 from intersecting (~8 marks)
#   IN-set: further prunes keys 1000..3999 (set has only 500..999)
#   Non-intersecting parts (4000..5499) survive PK, contribute to Union
#   status='active': filters ~50% within the set-selected range

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_combined;
    CREATE TABLE t_lazy_final_combined
    (
        key UInt64,
        version UInt64,
        status String,
        value UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY key
    SETTINGS index_granularity = 128;

    SYSTEM STOP MERGES t_lazy_final_combined;

    -- Intersecting pair: keys 0..3999 in two parts with different versions.
    -- status='active' only for even keys < 1000, 'inactive' otherwise.
    INSERT INTO t_lazy_final_combined SELECT
        number, 1,
        if(number < 1000 AND number % 2 = 0, 'active', 'inactive'),
        number
    FROM numbers(4000);

    INSERT INTO t_lazy_final_combined SELECT
        number, 2,
        if(number < 1000 AND number % 2 = 0, 'active', 'inactive'),
        number * 10
    FROM numbers(4000);

    -- Non-intersecting parts: keys 4000..4999 and 5000..5999.
    -- All inactive (outside the target range).
    INSERT INTO t_lazy_final_combined SELECT
        number + 4000, 1, 'inactive', number + 4000
    FROM numbers(1000);

    INSERT INTO t_lazy_final_combined SELECT
        number + 5000, 1, 'inactive', number + 5000
    FROM numbers(1000);
"

## Test 1: Correctness — results must match with and without optimization.
echo "=== Correctness ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 2: Plan has both Union (non-intersecting split) and InputSelector.
echo "=== Plan structure ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'Union'

$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'InputSelector'

## Test 3: Verify PK pruning and IN-set pruning.
## pk_filtered_marks is the baseline after WHERE's PK condition.
## selected_marks is after the IN-set filter — should be less than pk_filtered_marks.
echo "=== PK and IN-set pruning ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='trace' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Lazy FINAL enabled' \
    | sed 's/.*total_marks=/total_marks=/' \
    | sed -E 's/set_rows=[0-9]+/set_rows=N/' \
    | head -1

## Test 4: Verify the "Selected" line shows reduced parts/marks after IN-set index.
echo "=== Selected after index ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

## Test 5: Fallback path still correct.
echo "=== Fallback (set truncated) ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10
"

## Test 6: Full EXPLAIN showing all plan components.
echo "=== Full EXPLAIN ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN indexes=1
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 7: Set plan's ReadFromMergeTree should only read columns needed for
## set building (PK + filter columns), not query output columns like 'value'.
echo "=== Set plan reads only needed columns ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN header=1
    SELECT count(), sum(value) FROM t_lazy_final_combined FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | sed -n '/CreatingSet/{n; :a; /ReadFromMergeTree/{n; p; q}; n; ba}'

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_combined"
