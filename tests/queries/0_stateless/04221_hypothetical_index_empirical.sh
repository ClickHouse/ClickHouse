#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: hypothetical indexes are session-scoped and not replicated

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_emp;
    CREATE TABLE t_hypo_emp (a UInt64, b UInt64, c String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

    -- 100 granules of 100 rows; b = a (sorted), c = a % 100 (every granule has every value)
    INSERT INTO t_hypo_emp SELECT number, number, toString(number % 100) FROM numbers(10000);
"

# minmax on sorted b: WHERE b = 42 matches one granule -> 99% skip
echo "--- minmax on sorted column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 42;
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# bloom_filter on uniform c: every granule has every value -> 0% skip
echo "--- bloom_filter on uniform column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo_emp (c) TYPE bloom_filter(0.01) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE c = '42';
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# minmax range: WHERE b in [500, 1500) matches 10/100 granules -> 90% skip
echo "--- minmax range query ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b >= 500 AND b < 1500;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# set(200) on sorted b: WHERE b = 5000 matches one granule -> 99% skip
echo "--- set index on sorted column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_emp (b) TYPE set(200) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 5000;
" | grep -E '^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# GRANULARITY 5: 100 data granules -> 20 index granules; WHERE b = 42 -> 19/20 = 95% skip
echo "--- index granularity 5 ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b5 ON t_hypo_emp (b) TYPE minmax GRANULARITY 5;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 42;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# PK prunes a > 5000 to 50 baseline marks; b < 100 never holds there -> 100% skip, 50/100 sampled
echo "--- empirical respects PK pruning ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_pk ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE a > 5000 AND b < 100;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# Real minmax on non-PK column `b` prunes 99 granules; the hypothetical on `c`
# then sees baseline = 1, so this exercises "baseline includes existing skip indexes"
echo "--- empirical with existing real index ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_real;
    CREATE TABLE t_hypo_real (a UInt64, b UInt64, c UInt64, INDEX idx_b_real b TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_hypo_real SELECT number, intDiv(number, 100), number FROM numbers(10000);
"

$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_c_hypo ON t_hypo_real (c) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_real WHERE b = 50 AND c = 7777;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_real"

# Partition pruning leaves only p=1 (all b=1); WHERE b = 0 -> 100% skip on baseline.
# Reading the whole table would give 50% — proves only baseline granules are materialized.
echo "--- proof: only baseline granules materialized ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_proof;
    CREATE TABLE t_hypo_proof (p UInt8, a UInt64, b UInt64)
    ENGINE = MergeTree PARTITION BY p ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

    INSERT INTO t_hypo_proof SELECT 0, number, 0 FROM numbers(5000);
    INSERT INTO t_hypo_proof SELECT 1, number, 1 FROM numbers(5000);

    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_proof (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_proof WHERE p = 1 AND b = 0;
" | grep -E '^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proof"

# Adaptive granularity + PK pruning (range not starting at mark 0) must not trip
# `Read N rows, more than requested to read: M`.
echo "--- non-uniform marks + PK pruning ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_split;
    CREATE TABLE t_hypo_split (a UInt64, b String, c Float64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 16384, min_bytes_for_wide_part = 0;
    INSERT INTO t_hypo_split SELECT number, repeat(toString(number), 10), number * 1.1 FROM numbers(1000);

    CREATE HYPOTHETICAL INDEX idx_a_minmax ON t_hypo_split (a) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_split WHERE a > 500;
" | grep -E '^\s+status:|^\s+source:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_split"

# Sparse-serialized columns must be materialized before the aggregator (no crash).
echo "--- empirical on sparse-serialized column ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_sparse;
    CREATE TABLE t_hypo_sparse (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.0;
    INSERT INTO t_hypo_sparse SELECT number, number % 3 = 0 ? 0 : number FROM numbers(1000);
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_sparse (b) TYPE set(200) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_sparse WHERE b = 5;
" | grep -E '^\s+status:|^\s+source:'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_sparse"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_emp"
