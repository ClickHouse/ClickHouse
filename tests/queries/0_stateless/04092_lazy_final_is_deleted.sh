#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-debug

# Test lazy FINAL optimization with ReplacingMergeTree that has
# version and is_deleted columns, with both intersecting and
# non-intersecting parts. Verifies:
# 1. Correctness with and without optimization
# 2. True path (optimization applied) and false path (fallback)
# 3. Query plan structure
# 4. is_deleted filtering for non-intersecting parts

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_del;
    CREATE TABLE t_lazy_final_del
    (
        key UInt64,
        version UInt64,
        is_deleted UInt8,
        category String,
        value UInt64
    )
    ENGINE = ReplacingMergeTree(version, is_deleted)
    ORDER BY key
    SETTINGS index_granularity = 64;

    SYSTEM STOP MERGES t_lazy_final_del;

    -- Part 1: keys 0..499, some with category='target'
    INSERT INTO t_lazy_final_del SELECT
        number, 1, 0, if(number < 100, 'target', 'other'), number
    FROM numbers(500);

    -- Part 2: keys 200..699, overlaps with part 1 on 200..499 (intersecting)
    -- Updates some rows, marks some as deleted
    INSERT INTO t_lazy_final_del SELECT
        number + 200, 2,
        if(number < 50, 1, 0),  -- keys 200..249 deleted
        if(number < 150, 'target', 'other'),
        (number + 200) * 10
    FROM numbers(500);

    -- Part 3: keys 1000..1499, non-intersecting with parts 1 and 2
    -- Some rows marked as deleted
    INSERT INTO t_lazy_final_del SELECT
        number + 1000, 1,
        if(number >= 400, 1, 0),  -- keys 1400..1499 deleted
        if(number < 200, 'target', 'other'),
        (number + 1000) * 100
    FROM numbers(500);

    -- Part 4: keys 2000..2499, non-intersecting, all alive
    INSERT INTO t_lazy_final_del SELECT
        number + 2000, 1, 0,
        if(number < 50, 'target', 'other'),
        (number + 2000) * 100
    FROM numbers(500);
"

## Test 1: Correctness — optimization OFF vs ON (true path, min_filtered_ratio=0)
echo "=== Correctness: all rows ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 2: Correctness — without filter (is_deleted rows still filtered by FINAL)
echo "=== Correctness: no WHERE filter ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    SETTINGS query_plan_optimize_lazy_final = 0
"

## Test 3: Correctness — check specific deleted rows are gone
echo "=== Correctness: deleted rows absent ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE key >= 200 AND key < 250
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE key >= 200 AND key < 250
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 4: Correctness — non-intersecting deleted rows (keys 1400..1499)
echo "=== Correctness: non-intersecting deleted rows ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE key >= 1400 AND key < 1500
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE key >= 1400 AND key < 1500
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

## Test 5: True path — verify LazyFinalKeyAnalysisTransform logs "enabled"
echo "=== True path: optimization enabled ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='trace' 2>&1 \
    | grep -c 'LazyFinalKeyAnalysisTransform.*Lazy FINAL enabled'

## Test 6: False path (set truncated) — results still correct
echo "=== False path: set truncated ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10
"
# Verify it logged "disabled: set was truncated"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10
" --send_logs_level='trace' 2>&1 \
    | grep -c 'LazyFinalKeyAnalysisTransform.*truncated'

## Test 7: Plan check — optimization ON has InputSelector and Union (non-intersecting split)
echo "=== Plan: optimization ON ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'InputSelector'

$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'Union'

## Test 8: Plan check — optimization OFF has no InputSelector
echo "=== Plan: optimization OFF ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_del FINAL
    WHERE category = 'target'
    SETTINGS query_plan_optimize_lazy_final = 0
" | grep -c 'InputSelector'

## Test 9: All non-intersecting (separate table, no overlapping keys, with is_deleted)
echo "=== All non-intersecting with is_deleted ==="
$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_del_nooverlap;
    CREATE TABLE t_lazy_final_del_nooverlap
    (
        key UInt64,
        version UInt64,
        is_deleted UInt8,
        value UInt64
    )
    ENGINE = ReplacingMergeTree(version, is_deleted)
    ORDER BY key
    SETTINGS index_granularity = 64;

    SYSTEM STOP MERGES t_lazy_final_del_nooverlap;

    -- Part 1: keys 0..99
    INSERT INTO t_lazy_final_del_nooverlap SELECT number, 1, if(number < 10, 1, 0), number FROM numbers(100);
    -- Part 2: keys 1000..1099
    INSERT INTO t_lazy_final_del_nooverlap SELECT number + 1000, 1, if(number >= 90, 1, 0), number + 1000 FROM numbers(100);
"

# Without optimization
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del_nooverlap FINAL
    WHERE value > 0
    SETTINGS query_plan_optimize_lazy_final = 0
"
# With optimization — should take the all-non-intersecting fast path
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count(), sum(value) FROM t_lazy_final_del_nooverlap FINAL
    WHERE value > 0
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"
# Plan should have NO InputSelector (all non-intersecting → simple non-FINAL read)
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final_del_nooverlap FINAL
    WHERE value > 0
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
" | grep -c 'InputSelector'

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_del_nooverlap"

## Test 10: is_deleted column explicitly requested in query
echo "=== is_deleted in SELECT ==="
# Intersecting parts — is_deleted should appear in output, only rows with is_deleted=0
$CLICKHOUSE_CLIENT $settings -q "
    SELECT key, is_deleted FROM t_lazy_final_del FINAL
    WHERE key IN (200, 250, 1000, 1400) ORDER BY key
    SETTINGS query_plan_optimize_lazy_final = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT key, is_deleted FROM t_lazy_final_del FINAL
    WHERE key IN (200, 250, 1000, 1400) ORDER BY key
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0
"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_del"
