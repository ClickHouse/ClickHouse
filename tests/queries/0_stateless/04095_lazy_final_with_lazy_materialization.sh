#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-debug

# Test that optimizeLazyMaterialization and optimizeLazyFinal work together.
# Lazy materialization splits off heavy columns (payload), then lazy FINAL
# optimizes the remaining FINAL read with set-based index analysis.
#
# Data layout (ORDER BY key, index_granularity=128):
#   Parts 1,2: keys 0..3999 (overlapping → intersecting), ~32 marks each
#   Part 3: keys 4000..4999 (non-intersecting), ~8 marks
#   Part 4: keys 5000..5999 (non-intersecting), ~8 marks
#
# Query: SELECT key, value, payload FROM t FINAL
#        WHERE key >= 500 AND key < 5500 AND status = 'active'
#        ORDER BY sipHash64(value) LIMIT 5
#
# Expected optimizations:
#   1. Lazy materialization defers payload to LazilyReadFromMergeTree
#   2. Lazy FINAL builds a set from filtered keys, prunes via PK index
#   3. Non-intersecting split separates parts 3,4

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_both;
    CREATE TABLE t_lazy_both
    (
        key UInt64,
        version UInt64,
        status String,
        value UInt64,
        payload String
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY key
    SETTINGS index_granularity = 128;

    SYSTEM STOP MERGES t_lazy_both;

    -- Intersecting pair: keys 0..3999, status='active' concentrated in keys 0..999.
    INSERT INTO t_lazy_both SELECT
        number, 1,
        if(number < 1000 AND number % 2 = 0, 'active', 'inactive'),
        number, repeat('x', 200)
    FROM numbers(4000);

    INSERT INTO t_lazy_both SELECT
        number, 2,
        if(number < 1000 AND number % 2 = 0, 'active', 'inactive'),
        number * 10, repeat('y', 200)
    FROM numbers(4000);

    -- Non-intersecting parts: keys 4000..5999.
    INSERT INTO t_lazy_both SELECT
        number + 4000, 1, 'inactive', number + 4000, repeat('z', 200)
    FROM numbers(1000);

    INSERT INTO t_lazy_both SELECT
        number + 5000, 1, 'inactive', number + 5000, repeat('w', 200)
    FROM numbers(1000);
"

## Test 1: Correctness — results must match with both optimizations off vs on.
echo "=== Correctness ==="
$CLICKHOUSE_CLIENT $settings -q "
    SELECT key, value, substring(payload, 1, 1) AS p FROM t_lazy_both FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    ORDER BY sipHash64(value) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 0, query_plan_optimize_lazy_materialization = 0
"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT key, value, substring(payload, 1, 1) AS p FROM t_lazy_both FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    ORDER BY sipHash64(value) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0,
             query_plan_optimize_lazy_materialization = 1
"

## Test 2: Plan has both LazilyReadFromMergeTree (from lazy materialization)
## and InputSelector (from lazy FINAL).
echo "=== Plan structure ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT key, value, substring(payload, 1, 1) AS p FROM t_lazy_both FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    ORDER BY sipHash64(value) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0,
             query_plan_optimize_lazy_materialization = 1
" | grep -c 'LazilyReadFromMergeTree'

$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT key, value, substring(payload, 1, 1) AS p FROM t_lazy_both FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    ORDER BY sipHash64(value) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0,
             query_plan_optimize_lazy_materialization = 1
" | grep -c 'InputSelector'

## Test 3: Full EXPLAIN showing both optimizations.
echo "=== Full EXPLAIN ==="
$CLICKHOUSE_CLIENT $settings -q "
    EXPLAIN actions = 0
    SELECT key, value, substring(payload, 1, 1) AS p FROM t_lazy_both FINAL
    WHERE key >= 500 AND key < 5500 AND status = 'active'
    ORDER BY sipHash64(value) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 1,
             max_rows_for_lazy_final = 10000000,
             min_filtered_ratio_for_lazy_final = 0,
             query_plan_optimize_lazy_materialization = 1
"

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_both"
