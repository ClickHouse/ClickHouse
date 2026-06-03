#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings

# Test that EXPLAIN PIPELINE shows Concat (lazy partition reading) or
# MergingSortedTransform (standard merging) depending on whether the
# read_in_order_allow_per_partition_lazy_read optimization can be applied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="SET optimize_read_in_order=1, read_in_order_allow_per_partition_lazy_read=1"

# ============================================================================
# Table 1: PARTITION BY toYYYYMM(time) ORDER BY time
# Partition key monotonic in sort key at index 0 → no prefix to check.
# Optimization always applies (multiple partitions + setting enabled).
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_simple"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_simple (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY toYYYYMM(time) ORDER BY time"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_simple SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_simple SELECT toDateTime('2024-02-01') + number * 60, number + 1000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_simple SELECT toDateTime('2024-03-01') + number * 60, number + 2000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_simple FINAL"

# -- Test 1: Optimization enabled → Concat in pipeline
echo "test 1: enabled has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_simple
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

# -- Test 2: Optimization disabled → no Concat, has MergingSorted
echo "test 2: disabled no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_simple
    ORDER BY time ASC LIMIT 5
    SETTINGS max_threads=1, read_in_order_allow_per_partition_lazy_read=0" | grep -c "Concat" || true

echo "test 2: disabled has MergingSorted"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_simple
    ORDER BY time ASC LIMIT 5
    SETTINGS max_threads=1, read_in_order_allow_per_partition_lazy_read=0" | grep -c "MergingSortedTransform"

# -- Test 3: DESC order → still has Concat
echo "test 3: DESC has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_simple
    ORDER BY time DESC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

# -- Test 4: No WHERE at all → still has Concat (index 0, no prefix needed)
echo "test 4: no WHERE has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_simple
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_simple"

# ============================================================================
# Table 2: PARTITION BY toYYYYMM(time) ORDER BY (k1, k2, time)
# Partition key monotonic in sort key at index 2 (time).
# Prefix columns k1, k2 must be fixed by WHERE.
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_composite"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_composite (k1 String, k2 String, time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY toYYYYMM(time) ORDER BY (k1, k2, time)"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_composite SELECT 'aaa', 'bbb', toDateTime('2024-01-01') + number * 60, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_composite SELECT 'aaa', 'bbb', toDateTime('2024-02-01') + number * 60, number + 1000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_composite SELECT 'xxx', 'yyy', toDateTime('2024-01-01') + number * 60, number + 2000 FROM numbers(500)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_composite SELECT 'xxx', 'yyy', toDateTime('2024-02-01') + number * 60, number + 2500 FROM numbers(500)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_composite FINAL"

# -- Test 5: Both prefix columns fixed → Concat
echo "test 5: prefix fixed has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT k1, k2, time, val FROM t_lazy_pipeline_composite
    WHERE k1 = 'aaa' AND k2 = 'bbb'
    ORDER BY k1, k2, time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

# -- Test 6: Gap in prefix (k2 not fixed) → no Concat
echo "test 6: gap in prefix no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT k1, k2, time, val FROM t_lazy_pipeline_composite
    WHERE k1 = 'aaa'
    ORDER BY k1, k2, time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

# -- Test 7: No prefix fixed → no Concat
echo "test 7: no prefix no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT k1, k2, time, val FROM t_lazy_pipeline_composite
    WHERE time >= '2024-01-01'
    ORDER BY k1, k2, time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

# -- Test 8: IN on prefix column (not a single point) → no Concat
echo "test 8: IN prefix no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT k1, k2, time, val FROM t_lazy_pipeline_composite
    WHERE k1 IN ('aaa', 'xxx') AND k2 = 'bbb'
    ORDER BY k1, k2, time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

# -- Test 9: Range on prefix column (not a point) → no Concat
echo "test 9: range prefix no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT k1, k2, time, val FROM t_lazy_pipeline_composite
    WHERE k1 >= 'aaa' AND k2 = 'bbb'
    ORDER BY k1, k2, time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_composite"

# ============================================================================
# Table 3: Non-monotonic partition key — PARTITION BY toHour(time)
# toHour wraps around → not always-monotonic → no optimization.
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_non_mono"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_non_mono (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY toHour(time) ORDER BY time"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_non_mono SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(1440)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_non_mono SELECT toDateTime('2024-01-02') + number * 60, number + 1440 FROM numbers(1440)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_non_mono FINAL"

echo "test 10: non-monotonic no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_non_mono
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_non_mono"

# ============================================================================
# Table 4: Partition key on non-sorting-key column.
# PARTITION BY val % 3, ORDER BY time — val is not in sorting key.
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_non_sort"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_non_sort (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY val % 3 ORDER BY time"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_non_sort SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(3000)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_non_sort FINAL"

echo "test 11: non-sort-key partition no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_non_sort
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_non_sort"

# ============================================================================
# Table 5: Multi-column partition key — not supported.
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_multi_part"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_multi_part (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY (toYYYYMM(time), val % 2) ORDER BY time"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_multi_part SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_multi_part SELECT toDateTime('2024-02-01') + number * 60, number + 1000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_multi_part FINAL"

echo "test 12: multi-column partition key no Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_multi_part
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat" || true

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_multi_part"

# ============================================================================
# Table 6: PARTITION BY toDate(time) ORDER BY time — identity-like partition.
# toDate is monotonic in time → optimization applies.
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_todate"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_todate (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY toDate(time) ORDER BY time"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_todate SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(1440)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_todate SELECT toDateTime('2024-01-02') + number * 60, number + 1440 FROM numbers(1440)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_todate SELECT toDateTime('2024-01-03') + number * 60, number + 2880 FROM numbers(1440)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_todate FINAL"

echo "test 13: toDate partition has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_todate
    ORDER BY time ASC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_todate"

# ============================================================================
# Table 7: PARTITION BY toYYYYMM(time) ORDER BY time DESC (reversed key).
# allow_experimental_reverse_key + ORDER BY time DESC query
# ============================================================================

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_lazy_pipeline_reverse_key"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_lazy_pipeline_reverse_key (time DateTime, val UInt64)
    ENGINE = MergeTree PARTITION BY toYYYYMM(time) ORDER BY time DESC
    SETTINGS allow_experimental_reverse_key = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_reverse_key SELECT toDateTime('2024-01-01') + number * 60, number FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_reverse_key SELECT toDateTime('2024-02-01') + number * 60, number + 1000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_lazy_pipeline_reverse_key SELECT toDateTime('2024-03-01') + number * 60, number + 2000 FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_lazy_pipeline_reverse_key FINAL"

# -- Test 14: Optimization still applies
echo "test 14: reversed key DESC has Concat"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    EXPLAIN PIPELINE SELECT time, val FROM t_lazy_pipeline_reverse_key
    ORDER BY time DESC LIMIT 5 SETTINGS max_threads=1" | grep -c "Concat"

# -- Test 15: Correctness — ORDER BY time DESC LIMIT 5 must return the newest rows (March 2024)
echo "test 15: reversed key DESC correctness"
$CLICKHOUSE_CLIENT -q "
    $SETTINGS;
    SELECT toYYYYMM(time) AS ym FROM t_lazy_pipeline_reverse_key
    ORDER BY time DESC LIMIT 5" | sort -u

$CLICKHOUSE_CLIENT -q "DROP TABLE t_lazy_pipeline_reverse_key"
