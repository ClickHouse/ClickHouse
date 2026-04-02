#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Test that lazy FINAL optimization applies index analysis at runtime
# to narrow down the read ranges for the optimized branch.
# We check LazyReadReplacingFinalSource log messages to verify PK pruning.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

## Test 1: Single-column PK
echo "=== Single-column PK ==="

$CLICKHOUSE_CLIENT -q "
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
    INSERT INTO t_lazy_final_index SELECT number, 1, if(number < 100, 'target', 'other'), repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 1000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 2000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 3000, 1, 'other', repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_lazy_final_index SELECT number + 4000, 1, 'other', repeat('x', 100) FROM numbers(1000);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_index FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_lazy_final_index FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyReadReplacingFinalSource.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_final_index"


## Test 2: Tuple PK (multi-column primary key)
echo "=== Tuple PK ==="

$CLICKHOUSE_CLIENT -q "
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

    -- 5 parts, each in a different category. Only category=0 has 'target'.
    INSERT INTO t_lazy_final_tuple_pk SELECT 0, number, 1, if(number < 50, 'target', 'other'), repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 1, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 2, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 3, number, 1, 'other', repeat('y', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_tuple_pk SELECT 4, number, 1, 'other', repeat('y', 100) FROM numbers(500);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_lazy_final_tuple_pk FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyReadReplacingFinalSource.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_final_tuple_pk"


## Test 3: PK is a prefix of ORDER BY
echo "=== PK prefix of ORDER BY ==="

$CLICKHOUSE_CLIENT -q "
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

    -- 5 parts, each in a different category. Only category=0 has 'target'.
    INSERT INTO t_lazy_final_pk_prefix SELECT 0, number, 1, if(number < 50, 'target', 'other'), repeat('z', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_pk_prefix SELECT 1, number, 1, 'other', repeat('z', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_pk_prefix SELECT 2, number, 1, 'other', repeat('z', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_pk_prefix SELECT 3, number, 1, 'other', repeat('z', 100) FROM numbers(500);
    INSERT INTO t_lazy_final_pk_prefix SELECT 4, number, 1, 'other', repeat('z', 100) FROM numbers(500);
"

echo "-- correctness"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 0"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(payload)) FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target' SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000"

echo "-- index analysis (PK is only category, so set has fewer columns)"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_lazy_final_pk_prefix FINAL WHERE status = 'target'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000
" --send_logs_level='debug' 2>&1 \
    | grep 'LazyReadReplacingFinalSource.*Selected' \
    | sed 's/.*Selected /Selected /' \
    | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_final_pk_prefix"
