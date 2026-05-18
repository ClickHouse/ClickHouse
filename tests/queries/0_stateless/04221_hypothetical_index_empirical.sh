#!/usr/bin/env bash
# Tags: no-replicated-database, no-random-merge-tree-settings
# no-random-merge-tree-settings: needs fixed index_granularity

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_emp;
    CREATE TABLE t_hypo_emp (a UInt64, b UInt64, c String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

    -- 10000 rows: 100 granules of 100 rows each
    -- b = a (sorted, unique range per granule)
    -- c = toString(a % 100) (uniformly distributed, every granule has every value)
    INSERT INTO t_hypo_emp SELECT number, number, toString(number % 100) FROM numbers(10000);
"

# =========================================================
# Empirical: minmax on sorted column - high skip ratio
# WHERE b = 42 matches only granule 0 → 99% skip
# =========================================================
echo "--- minmax on sorted column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 42;
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# =========================================================
# Empirical: bloom_filter on uniform column - 0% skip
# Every granule has all 100 distinct values of c → bloom can't skip
# =========================================================
echo "--- bloom_filter on uniform column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo_emp (c) TYPE bloom_filter(0.01) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE c = '42';
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# =========================================================
# Empirical: minmax range query - matches 10 of 100 granules
# WHERE b >= 500 AND b < 1500 → granules 5..14 → 10 granules → 90% skip
# =========================================================
echo "--- minmax range query ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b >= 500 AND b < 1500;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# =========================================================
# Empirical: set index on sorted column
# set(200) on b: each granule has 100 distinct values,
# WHERE b = 5000 matches one granule → 99% skip
# =========================================================
echo "--- set index on sorted column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_emp (b) TYPE set(200) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 5000;
" | grep -E '^\s+skip_ratio:|^\s+source:|^\s+sampled_marks:'

# =========================================================
# Empirical: index granularity > 1 (each index granule covers multiple data granules)
# GRANULARITY 5 → 100 data granules / 5 = 20 index granules
# WHERE b = 42 → matches 1st index granule → 19/20 = 95% skip
# =========================================================
echo "--- index granularity 5 ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b5 ON t_hypo_emp (b) TYPE minmax GRANULARITY 5;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 42;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# =========================================================
# Empirical respects PK pruning: only baseline marks are processed
# ORDER BY a, so WHERE a > 5000 prunes first ~50 granules via PK
# Among remaining granules (a>5000), b is in [5001..9999], so b < 100 is never true
# → hypothetical minmax should skip 100% of baseline marks
# sampled_marks should be 50 / 100: 50 baseline marks scanned out of 100 in the table
# =========================================================
echo "--- empirical respects PK pruning ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_pk ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE a > 5000 AND b < 100;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# Real minmax on b prunes 99 granules; hypothetical on c then sees baseline = 1
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

# =========================================================
# Proof that empirical mode only materializes on baseline granules
#
# Partitioned table (PARTITION BY p):
#   Partition p=0: b = 0 everywhere (50 granules)
#   Partition p=1: b = 1 everywhere (50 granules)
#
# Query: WHERE p = 1 AND b = 0
#   Partition pruning removes p=0 entirely → baseline = p=1 only (all b=1)
#   Hypothetical minmax on b: b=0 not in any p=1 granule → 100% skip
#
# If whole table was read: p=0 has b=0 → NOT skipped, p=1 has b=1 → skipped.
#   skip_ratio = 50%
# With only baseline: all granules have b=1, skip_ratio = 100%
# =========================================================
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

# =========================================================
# Non-uniform mark sizes + PK pruning must not trip
# `Read N rows, more than requested to read: M`
# Adaptive granularity with a small index_granularity_bytes
# splits 1000 rows into several marks of differing row counts;
# `WHERE a > 500` removes early marks so the surviving range
# does not start at mark 0
# =========================================================
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
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_emp"
