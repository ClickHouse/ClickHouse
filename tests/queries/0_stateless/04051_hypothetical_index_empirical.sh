#!/usr/bin/env bash
# Tags: no-replicated-database, no-random-merge-tree-settings
# no-random-merge-tree-settings: test requires deterministic index_granularity

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
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:'

# =========================================================
# Empirical: bloom_filter on uniform column - 0% skip
# Every granule has all 100 distinct values of c → bloom can't skip
# =========================================================
echo "--- bloom_filter on uniform column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_c ON t_hypo_emp (c) TYPE bloom_filter(0.01) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE c = '42';
" | grep -E '^\s+status:|^\s+skip_ratio:|^\s+source:'

# =========================================================
# Empirical: minmax range query - matches 10 of 100 granules
# WHERE b >= 500 AND b < 1500 → granules 5..14 → 10 granules → 90% skip
# =========================================================
echo "--- minmax range query ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b >= 500 AND b < 1500;
" | grep -E '^\s+skip_ratio:'

# =========================================================
# Empirical: set index on sorted column
# set(200) on b: each granule has 100 distinct values,
# WHERE b = 5000 matches one granule → 99% skip
# =========================================================
echo "--- set index on sorted column ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_set ON t_hypo_emp (b) TYPE set(200) GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 5000;
" | grep -E '^\s+skip_ratio:|^\s+source:'

# =========================================================
# Empirical: index granularity > 1 (each index granule covers multiple data granules)
# GRANULARITY 5 → 100 data granules / 5 = 20 index granules
# WHERE b = 42 → matches 1st index granule → 19/20 = 95% skip
# =========================================================
echo "--- index granularity 5 ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b5 ON t_hypo_emp (b) TYPE minmax GRANULARITY 5;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE b = 42;
" | grep -E '^\s+skip_ratio:'

# =========================================================
# Empirical respects PK pruning: only baseline marks are processed
# ORDER BY a, so WHERE a > 5000 prunes first ~50 granules via PK.
# Among remaining granules (a>5000), b is in [5001..9999], so b < 100 is never true.
# → hypothetical minmax should skip 100% of baseline marks.
# sampled_marks should equal baseline marks (50), not total marks (100).
# =========================================================
echo "--- empirical respects PK pruning ---"
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_pk ON t_hypo_emp (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_emp WHERE a > 5000 AND b < 100;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

# =========================================================
# Empirical with existing real skip index:
# hypothetical index only processes marks surviving both PK and real index.
# =========================================================
echo "--- empirical with existing real index ---"
$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS t_hypo_real;
    CREATE TABLE t_hypo_real (a UInt64, b UInt64, INDEX idx_a_real a TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
    INSERT INTO t_hypo_real SELECT number, number FROM numbers(10000);
"

# With real minmax on a and PK on a, WHERE a > 5000 is handled by PK.
# WHERE b = 7777 is tested by hypothetical minmax on b.
# sampled_marks = baseline marks ≈ 50
$CLICKHOUSE_CLIENT -n -q "
    CREATE HYPOTHETICAL INDEX idx_b_hypo ON t_hypo_real (b) TYPE minmax GRANULARITY 1;
    EXPLAIN WHATIF SELECT * FROM t_hypo_real WHERE a > 5000 AND b = 7777;
" | grep -E '^\s+skip_ratio:|^\s+sampled_marks:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_real"

# =========================================================
# Proof that empirical mode only materializes on baseline granules.
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
#   skip_ratio = 50%.
# With only baseline: all granules have b=1, skip_ratio = 100%.
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
" | grep -E '^\s+skip_ratio:|^\s+source:'

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_proof"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_hypo_emp"
