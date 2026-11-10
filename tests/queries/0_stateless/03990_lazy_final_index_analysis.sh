#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Test that lazy FINAL optimization applies index analysis at runtime
# to narrow down the read ranges for the optimized branch.
# We check LazyFinalKeyAnalysisTransform trace messages to verify PK pruning.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

settings="--enable_analyzer=1"

## Test 1: Single-column PK
echo "=== Single-column PK ==="

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_index;
    CREATE TABLE t_lazy_final_index
    (
        key UInt64,
        version UInt64,
        status String,
        payload String
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY key
    SETTINGS index_granularity = 64;

    SYSTEM STOP MERGES t_lazy_final_index;
    -- Two overlapping parts on key range 0..999, plus non-overlapping parts.
    INSERT INTO t_lazy_final_index SELECT number, 1, if(number < 100, 'target', 'other'), repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number, 2, if(number < 100, 'target', 'other'), repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 1000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 2000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 3000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 4000, 1, 'other', repeat('x', 100) FROM numbers(1000);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_index FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_index"


## Test 2: Tuple PK (multi-column primary key)
echo "=== Tuple PK ==="

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_tuple_pk;
    CREATE TABLE t_lazy_final_tuple_pk
    (
        category UInt64,
        key UInt64,
        version UInt64,
        status String,
        payload String
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (category, key)
    SETTINGS index_granularity = 64;

    SYSTEM STOP MERGES t_lazy_final_tuple_pk;

    -- 6 parts: two overlapping on category=0, rest non-overlapping. Only category=0 has 'target'.
    INSERT INTO t_lazy_final_tuple_pk SELECT 0, number, 1, if(number < 50, 'target', 'other'), repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 0, number, 2, if(number < 50, 'target', 'other'), repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 1, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 2, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 3, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 4, number, 1, 'other', repeat('y', 100) FROM numbers(500);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_tuple_pk"


## Test 3: PK is a prefix of ORDER BY
echo "=== PK prefix of ORDER BY ==="

$CLICKHOUSE_CLIENT $settings -q "
    DROP TABLE IF EXISTS t_lazy_final_pk_prefix;
    CREATE TABLE t_lazy_final_pk_prefix
    (
        category UInt64,
        key UInt64,
        version UInt64,
        status String,
        payload String
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (category, key)
    PRIMARY KEY category
    SETTINGS index_granularity = 64;

    SYSTEM STOP MERGES t_lazy_final_pk_prefix;

    -- Two parts with all 5 categories (fully overlapping), so all parts are intersecting.
    -- Only category=0 has 'target', so the PK set {0} should prune marks for categories 1-4.
    INSERT INTO t_lazy_final_pk_prefix SELECT number % 5, number, 1, if(number % 5 = 0 AND number < 250, 'target', 'other'), repeat('z', 100) FROM numbers(2500);
    INSERT INTO t_lazy_final_pk_prefix SELECT number % 5, number, 2, if(number % 5 = 0 AND number < 250, 'target', 'other'), repeat('z', 100) FROM numbers(2500);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT $settings -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis (PK is only category, so set has fewer columns)"
$CLICKHOUSE_CLIENT $settings -q "
    SELECT count() FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyFinalKeyAnalysisTransform.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE t_lazy_final_pk_prefix"
