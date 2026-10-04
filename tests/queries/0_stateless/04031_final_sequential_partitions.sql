-- Test that optimize_final_sequential_partitions processes partitions sequentially
-- via ConcatProcessor, allowing early termination with LIMIT.
-- The partition order is determined automatically from minmax index values of the
-- partition-related sorting key column.

SET enable_analyzer = 1;

-- ============================================================
-- Setup: AggregatingMergeTree with DESC reverse key, partitioned by month
-- ============================================================

DROP TABLE IF EXISTS t_seq_part;

CREATE TABLE t_seq_part
(
    token String,
    pair String,
    unix_time DateTime,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(unix_time)
ORDER BY (token, pair, unix_time DESC)
SETTINGS allow_experimental_reverse_key = 1;

SYSTEM STOP MERGES t_seq_part;

-- Insert data spanning 3 months (3 partitions), two batches each.
INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-02-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-02-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-03-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part
    SELECT 'BTC', 'USD', toDateTime('2024-03-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

-- Insert different token to verify PREWHERE filtering
INSERT INTO t_seq_part
    SELECT 'ETH', 'USD', toDateTime('2024-03-01 00:00:00') + number, sumState(toUInt64(10))
    FROM numbers(50) GROUP BY number;

-- ============================================================
-- Part 1: Correctness - newest 5 rows (DESC)
-- ============================================================

-- Set do_not_merge_across_partitions_select_final explicitly because the flaky
-- check randomly turns off enable_automatic_decision_for_merging_across_partitions_for_final,
-- which makes the automatic detection return false and disables sequential
-- partition processing (no ConcatProcessor would be added).

SELECT '--- Part 1: Correctness - newest 5 rows (DESC) ---';
SELECT
    unix_time,
    finalizeAggregation(val) AS total
FROM t_seq_part FINAL
PREWHERE token = 'BTC' AND pair = 'USD'
ORDER BY unix_time DESC
LIMIT 5
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

-- ============================================================
-- Part 2: Pipeline contains Concat
-- ============================================================

SELECT '--- Part 2: Pipeline contains Concat ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT
        unix_time,
        finalizeAggregation(val) AS total
    FROM t_seq_part FINAL
    PREWHERE token = 'BTC' AND pair = 'USD'
    ORDER BY unix_time DESC
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

-- ============================================================
-- Part 3: Cross-partition boundary (10 rows)
-- ============================================================

SELECT '--- Part 3: Cross-partition boundary (10 rows) ---';
SELECT
    unix_time,
    finalizeAggregation(val) AS total
FROM t_seq_part FINAL
PREWHERE token = 'BTC' AND pair = 'USD'
ORDER BY unix_time DESC
LIMIT 10
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

-- ============================================================
-- Part 4: ASC direction - oldest 5 rows
-- ============================================================

SELECT '--- Part 4: ASC direction - oldest 5 rows ---';

DROP TABLE IF EXISTS t_seq_part_asc;

CREATE TABLE t_seq_part_asc
(
    ts DateTime,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

SYSTEM STOP MERGES t_seq_part_asc;

INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-02-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-02-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-03-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_part_asc
    SELECT toDateTime('2024-03-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

SELECT
    ts,
    finalizeAggregation(val) AS total
FROM t_seq_part_asc FINAL
ORDER BY ts ASC
LIMIT 5
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

-- ============================================================
-- Part 5: Pipeline contains Concat (ASC)
-- ============================================================

SELECT '--- Part 5: Pipeline contains Concat (ASC) ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT
        ts,
        finalizeAggregation(val) AS total
    FROM t_seq_part_asc FINAL
    ORDER BY ts ASC
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

-- ============================================================
-- Part 6: No Concat without limit pushdown
-- ============================================================

SELECT '--- Part 6: No Concat without limit pushdown ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT
        ts,
        finalizeAggregation(val) AS total
    FROM t_seq_part_asc FINAL
    ORDER BY ts ASC
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 0,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

-- ============================================================
-- Part 7: Uncorrelated keys (no sequential partitions)
-- ============================================================

SELECT '--- Part 7: Uncorrelated keys (no sequential partitions) ---';

DROP TABLE IF EXISTS t_seq_uncorrelated;

CREATE TABLE t_seq_uncorrelated
(
    part_key UInt64,
    sort_key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY part_key
ORDER BY sort_key;

SYSTEM STOP MERGES t_seq_uncorrelated;

-- Insert into 3 partitions with non-overlapping sort keys.
-- Partition 1: sort_key 0..49, Partition 2: sort_key 50..99, Partition 3: sort_key 100..149
INSERT INTO t_seq_uncorrelated
    SELECT 1, number, sumState(toUInt64(1))
    FROM numbers(50) GROUP BY number;
INSERT INTO t_seq_uncorrelated
    SELECT 1, number, sumState(toUInt64(2))
    FROM numbers(50) GROUP BY number;

INSERT INTO t_seq_uncorrelated
    SELECT 2, 50 + number, sumState(toUInt64(1))
    FROM numbers(50) GROUP BY number;
INSERT INTO t_seq_uncorrelated
    SELECT 2, 50 + number, sumState(toUInt64(2))
    FROM numbers(50) GROUP BY number;

INSERT INTO t_seq_uncorrelated
    SELECT 3, 100 + number, sumState(toUInt64(1))
    FROM numbers(50) GROUP BY number;
INSERT INTO t_seq_uncorrelated
    SELECT 3, 100 + number, sumState(toUInt64(2))
    FROM numbers(50) GROUP BY number;

-- Correctness: results should still be correct via parallel Union.
-- The partition key column (part_key) is not in the sorting key (sort_key),
-- so the minmax-based algorithm cannot determine partition order.
SELECT
    sort_key,
    finalizeAggregation(val) AS total
FROM t_seq_uncorrelated FINAL
ORDER BY sort_key ASC
LIMIT 5
SETTINGS optimize_read_in_order = 1,
         optimize_final_limit_pushdown = 1,
         optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1;

-- ============================================================
-- Part 8: No Concat (uncorrelated keys)
-- ============================================================

SELECT '--- Part 8: No Concat (uncorrelated keys) ---';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT
        sort_key,
        finalizeAggregation(val) AS total
    FROM t_seq_uncorrelated FINAL
    ORDER BY sort_key ASC
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

-- ============================================================
-- Part 9: Single partition - no Concat
-- ============================================================

SELECT '--- Part 9: Single partition - no Concat ---';

DROP TABLE IF EXISTS t_seq_single;

CREATE TABLE t_seq_single
(
    ts DateTime,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

SYSTEM STOP MERGES t_seq_single;

INSERT INTO t_seq_single
    SELECT toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(1))
    FROM numbers(100) GROUP BY number;
INSERT INTO t_seq_single
    SELECT toDateTime('2024-01-01 00:00:00') + number, sumState(toUInt64(2))
    FROM numbers(100) GROUP BY number;

SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT
        ts,
        finalizeAggregation(val) AS total
    FROM t_seq_single FINAL
    ORDER BY ts ASC
    LIMIT 5
    SETTINGS optimize_read_in_order = 1,
             optimize_final_limit_pushdown = 1,
             optimize_final_sequential_partitions = 1,
             do_not_merge_across_partitions_select_final = 1
)
WHERE explain LIKE '%Concat%';

DROP TABLE t_seq_part;
DROP TABLE t_seq_part_asc;
DROP TABLE t_seq_uncorrelated;
DROP TABLE t_seq_single;
