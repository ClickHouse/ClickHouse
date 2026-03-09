-- Test that LIMIT is pushed down into FINAL merge algorithms,
-- so the merge stops early once enough rows have been produced.

SET optimize_final_limit_pushdown = 1;

-- ============================================================
-- Part 1: AggregatingMergeTree with forward sorting key
-- ============================================================

DROP TABLE IF EXISTS t_agg_final_limit;

CREATE TABLE t_agg_final_limit
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

-- Insert two batches so there are overlapping parts to merge during FINAL.
INSERT INTO t_agg_final_limit
    SELECT number, sumState(toUInt64(1)) FROM numbers(1000) GROUP BY number;
INSERT INTO t_agg_final_limit
    SELECT number, sumState(toUInt64(2)) FROM numbers(1000) GROUP BY number;

-- Correctness: FINAL + ORDER BY + LIMIT should return the first 5 keys with sum=3.
SELECT 'agg_forward_on';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

-- Same query with the optimization disabled should produce identical results.
SELECT 'agg_forward_off';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 2: AggregatingMergeTree with reverse sorting key (DESC)
-- ============================================================

DROP TABLE IF EXISTS t_agg_final_limit_desc;

CREATE TABLE t_agg_final_limit_desc
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key DESC
SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO t_agg_final_limit_desc
    SELECT number, sumState(toUInt64(1)) FROM numbers(1000) GROUP BY number;
INSERT INTO t_agg_final_limit_desc
    SELECT number, sumState(toUInt64(2)) FROM numbers(1000) GROUP BY number;

-- With reverse key, ORDER BY key DESC matches the physical order.
SELECT 'agg_reverse_on';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit_desc FINAL
ORDER BY key DESC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'agg_reverse_off';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit_desc FINAL
ORDER BY key DESC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 3: Correctness across partitions
-- With do_not_merge_across_partitions_select_final, each partition
-- gets its own merge with limit, then results are combined.
-- ============================================================

DROP TABLE IF EXISTS t_agg_final_limit_part;

CREATE TABLE t_agg_final_limit_part
(
    part_key UInt64,
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY part_key
ORDER BY key
SETTINGS do_not_merge_across_partitions_select_final = 1;

-- Two partitions, two parts each.
INSERT INTO t_agg_final_limit_part
    SELECT 1, number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_agg_final_limit_part
    SELECT 1, number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;
INSERT INTO t_agg_final_limit_part
    SELECT 2, number + 100, sumState(toUInt64(10)) FROM numbers(100) GROUP BY number;
INSERT INTO t_agg_final_limit_part
    SELECT 2, number + 100, sumState(toUInt64(20)) FROM numbers(100) GROUP BY number;

-- LIMIT 5 should return the first 5 keys across both partitions (keys 0-4).
SELECT 'agg_partitioned_on';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit_part FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'agg_partitioned_off';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit_part FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 4: Boundary — LIMIT 1
-- ============================================================

SELECT 'limit_1_agg';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit FINAL
ORDER BY key ASC
LIMIT 1
SETTINGS optimize_final_limit_pushdown = 1;

-- ============================================================
-- Part 5: LIMIT larger than total rows — should return all rows
-- ============================================================

DROP TABLE IF EXISTS t_small_agg;

CREATE TABLE t_small_agg
(
    key UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO t_small_agg SELECT number, sumState(toUInt64(1)) FROM numbers(3) GROUP BY number;
INSERT INTO t_small_agg SELECT number, sumState(toUInt64(2)) FROM numbers(3) GROUP BY number;

SELECT 'limit_exceeds_rows';
SELECT key, finalizeAggregation(val)
FROM t_small_agg FINAL
ORDER BY key ASC
LIMIT 100
SETTINGS optimize_final_limit_pushdown = 1;

-- ============================================================
-- Part 6: Compound sorting key (multi-column ORDER BY)
-- ============================================================

DROP TABLE IF EXISTS t_compound_key;

CREATE TABLE t_compound_key
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

INSERT INTO t_compound_key
    SELECT number % 10, number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number % 10, number;
INSERT INTO t_compound_key
    SELECT number % 10, number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number % 10, number;

SELECT 'compound_key_on';
SELECT a, b, finalizeAggregation(val)
FROM t_compound_key FINAL
ORDER BY a, b
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'compound_key_off';
SELECT a, b, finalizeAggregation(val)
FROM t_compound_key FINAL
ORDER BY a, b
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 7: ReplacingMergeTree — limit must NOT corrupt results
-- ReplacingMergeTree needs to see all rows to pick the latest version,
-- so the optimization must not cause wrong results even if enabled.
-- ============================================================

DROP TABLE IF EXISTS t_replacing_final_limit;

CREATE TABLE t_replacing_final_limit
(
    key UInt64,
    version UInt64,
    value String
)
ENGINE = ReplacingMergeTree(version)
ORDER BY key;

INSERT INTO t_replacing_final_limit SELECT number, 1, 'old' FROM numbers(100);
INSERT INTO t_replacing_final_limit SELECT number, 2, 'new' FROM numbers(100);

-- All 5 rows must show version=2, value='new' regardless of optimization.
SELECT 'replacing_on';
SELECT key, version, value
FROM t_replacing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'replacing_off';
SELECT key, version, value
FROM t_replacing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 8: CollapsingMergeTree — limit must NOT corrupt results
-- ============================================================

DROP TABLE IF EXISTS t_collapsing_final_limit;

CREATE TABLE t_collapsing_final_limit
(
    key UInt64,
    value UInt64,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY key;

-- Insert rows: for keys 0-99 insert (+1) then (-1) then (+1) again.
-- After collapsing, each key should have value from the last +1 row.
INSERT INTO t_collapsing_final_limit SELECT number, number * 10, 1 FROM numbers(100);
INSERT INTO t_collapsing_final_limit SELECT number, number * 10, -1 FROM numbers(100);
INSERT INTO t_collapsing_final_limit SELECT number, number * 100, 1 FROM numbers(100);

SELECT 'collapsing_on';
SELECT key, value
FROM t_collapsing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'collapsing_off';
SELECT key, value
FROM t_collapsing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 9: No ORDER BY — optimization should not apply
-- (no input_order_info means limit=0), results must be correct.
-- ============================================================

-- Without ORDER BY, output order is non-deterministic.
-- We just check it doesn't crash and returns the right count.
SELECT 'no_order_by_count';
SELECT count()
FROM (
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
);

-- ============================================================
-- Part 10: ORDER BY not matching sorting key — optimization should not apply
-- ============================================================

SELECT 'order_mismatch';
SELECT count()
FROM (
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    ORDER BY finalizeAggregation(val) DESC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
);

-- ============================================================
-- Part 11: LIMIT with OFFSET
-- ============================================================

SELECT 'offset_limit_on';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit FINAL
ORDER BY key ASC
LIMIT 3 OFFSET 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'offset_limit_off';
SELECT key, finalizeAggregation(val)
FROM t_agg_final_limit FINAL
ORDER BY key ASC
LIMIT 3 OFFSET 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 12: SummingMergeTree — limit pushdown should work
-- SummingMergeTree sums values per key group, same as Aggregating.
-- Once a group finishes, the result is final — safe to terminate early.
-- ============================================================

DROP TABLE IF EXISTS t_summing_final_limit;

CREATE TABLE t_summing_final_limit
(
    key UInt64,
    value UInt64
)
ENGINE = SummingMergeTree
ORDER BY key;

INSERT INTO t_summing_final_limit SELECT number, 1 FROM numbers(1000);
INSERT INTO t_summing_final_limit SELECT number, 2 FROM numbers(1000);

SELECT 'summing_on';
SELECT key, value
FROM t_summing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'summing_off';
SELECT key, value
FROM t_summing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- ============================================================
-- Part 13: EXPLAIN PIPELINE — verify the limit is visible
-- The optimization should add "Description: limit N" to the
-- merging transform in EXPLAIN PIPELINE output.
-- ============================================================

-- AggregatingMergeTree: optimization ON — must show limit
SELECT 'explain_agg_on';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- AggregatingMergeTree: optimization OFF — no Description with limit
SELECT 'explain_agg_off';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 0
)
WHERE explain LIKE '%Description: limit%';

-- SummingMergeTree: optimization ON — must show limit
SELECT 'explain_summing_on';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_summing_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- SummingMergeTree: optimization OFF — no Description with limit
SELECT 'explain_summing_off';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_summing_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 0
)
WHERE explain LIKE '%Description: limit%';

-- ReplacingMergeTree: limit must NOT appear (even with setting ON)
SELECT 'explain_replacing_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, version, value
    FROM t_replacing_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
)
WHERE explain LIKE '%Description: limit%';

-- CollapsingMergeTree: limit must NOT appear (even with setting ON)
SELECT 'explain_collapsing_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_collapsing_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
)
WHERE explain LIKE '%Description: limit%';

-- Reverse key (ORDER BY key DESC): with reverse key table, ORDER BY key DESC
-- matches the physical order, so read-in-order + limit pushdown should apply.
SELECT 'explain_reverse_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit_desc FINAL
    ORDER BY key DESC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- No ORDER BY: optimization should not apply
SELECT 'explain_no_orderby_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
)
WHERE explain LIKE '%Description: limit%';

-- LIMIT with OFFSET: limit pushed down should be limit+offset = 8
SELECT 'explain_offset_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, finalizeAggregation(val)
    FROM t_agg_final_limit FINAL
    ORDER BY key ASC
    LIMIT 3 OFFSET 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 8%';

-- ============================================================
-- Part 14: PREWHERE filter allows limit pushdown
-- With compound key (a, b) and PREWHERE a = 1, the filter runs
-- within each part before the FINAL merge. All rows entering the
-- merge have the same prefix value, so stopping after N merged
-- rows is correct.
-- ============================================================

DROP TABLE IF EXISTS t_prewhere_asc;

CREATE TABLE t_prewhere_asc
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

INSERT INTO t_prewhere_asc
    SELECT number % 5, number, sumState(toUInt64(1)) FROM numbers(500) GROUP BY number % 5, number;
INSERT INTO t_prewhere_asc
    SELECT number % 5, number, sumState(toUInt64(2)) FROM numbers(500) GROUP BY number % 5, number;

-- Correctness: PREWHERE a = 1, ORDER BY b LIMIT 5 (ascending)
SELECT 'prewhere_asc_on';
SELECT b, finalizeAggregation(val)
FROM t_prewhere_asc FINAL
PREWHERE a = 1
ORDER BY b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'prewhere_asc_off';
SELECT b, finalizeAggregation(val)
FROM t_prewhere_asc FINAL
PREWHERE a = 1
ORDER BY b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear — PREWHERE runs before merge
SELECT 'explain_prewhere_asc_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT b, finalizeAggregation(val)
    FROM t_prewhere_asc FINAL
    PREWHERE a = 1
    ORDER BY b ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 15: PREWHERE filter with DESC key
-- Same scenario with reverse key — verify limit pushdown works.
-- ============================================================

DROP TABLE IF EXISTS t_prewhere_desc;

CREATE TABLE t_prewhere_desc
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a DESC, b DESC)
SETTINGS allow_experimental_reverse_key = 1;

INSERT INTO t_prewhere_desc
    SELECT number % 5, number, sumState(toUInt64(1)) FROM numbers(500) GROUP BY number % 5, number;
INSERT INTO t_prewhere_desc
    SELECT number % 5, number, sumState(toUInt64(2)) FROM numbers(500) GROUP BY number % 5, number;

-- Correctness: PREWHERE a = 1, ORDER BY b DESC LIMIT 5
SELECT 'prewhere_desc_on';
SELECT b, finalizeAggregation(val)
FROM t_prewhere_desc FINAL
PREWHERE a = 1
ORDER BY b DESC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'prewhere_desc_off';
SELECT b, finalizeAggregation(val)
FROM t_prewhere_desc FINAL
PREWHERE a = 1
ORDER BY b DESC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear — PREWHERE runs before merge
SELECT 'explain_prewhere_desc_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT b, finalizeAggregation(val)
    FROM t_prewhere_desc FINAL
    PREWHERE a = 1
    ORDER BY b DESC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 16: WHERE filter prevents limit pushdown (negative test)
-- Same scenario but with WHERE instead of PREWHERE. The filter
-- runs AFTER the FINAL merge, so pushing a limit into the merge
-- would produce wrong results. Verify no limit pushdown.
-- Note: disable optimize_move_to_prewhere to ensure the filter
-- stays as a FilterStep.
-- ============================================================

SELECT 'where_filter_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT b, finalizeAggregation(val)
    FROM t_prewhere_asc FINAL
    WHERE a = 1
    ORDER BY b ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_move_to_prewhere = 0
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Part 17: PREWHERE + WHERE together prevents limit pushdown
-- Three-column key (a, b, c): PREWHERE fixes column a (runs
-- before the merge), but WHERE on b = 2 becomes a FilterStep
-- that runs AFTER the merge. Even though PREWHERE is present,
-- the FilterStep makes limit pushdown unsafe.
-- Using a fixed-point WHERE (b = 2) removes any doubt about
-- whether the failure is due to non-fixed-point range filtering.
-- ============================================================

DROP TABLE IF EXISTS t_prewhere_where;

CREATE TABLE t_prewhere_where
(
    a UInt64,
    b UInt64,
    c UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b, c);

-- 500 rows: a = number % 5, b = number % 10, c = number
INSERT INTO t_prewhere_where
    SELECT number % 5, number % 10, number, sumState(toUInt64(1)) FROM numbers(500) GROUP BY number % 5, number % 10, number;
INSERT INTO t_prewhere_where
    SELECT number % 5, number % 10, number, sumState(toUInt64(2)) FROM numbers(500) GROUP BY number % 5, number % 10, number;

SELECT 'prewhere_and_where_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT c, finalizeAggregation(val)
    FROM t_prewhere_where FINAL
    PREWHERE a = 1
    WHERE b = 2
    ORDER BY c ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_move_to_prewhere = 0
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Part 18: CoalescingMergeTree — limit pushdown should work
-- CoalescingMergeTree uses SummingSortedAlgorithm internally,
-- which we extended with limit support.
-- ============================================================

DROP TABLE IF EXISTS t_coalescing_final_limit;

CREATE TABLE t_coalescing_final_limit
(
    key UInt64,
    value UInt64
)
ENGINE = CoalescingMergeTree
ORDER BY key;

INSERT INTO t_coalescing_final_limit SELECT number, 1 FROM numbers(1000);
INSERT INTO t_coalescing_final_limit SELECT number, 2 FROM numbers(1000);

-- CoalescingMergeTree sums numeric columns per key group.
SELECT 'coalescing_on';
SELECT key, value
FROM t_coalescing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'coalescing_off';
SELECT key, value
FROM t_coalescing_final_limit FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear
SELECT 'explain_coalescing_on';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_coalescing_final_limit FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 19: VersionedCollapsingMergeTree — limit must NOT apply
-- This engine needs to see all versions to collapse correctly.
-- ============================================================

DROP TABLE IF EXISTS t_versioned_collapsing;

CREATE TABLE t_versioned_collapsing
(
    key UInt64,
    value UInt64,
    sign Int8,
    version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY key;

-- Insert: version 1 (+1), version 1 (-1 cancel), version 2 (+1 final).
INSERT INTO t_versioned_collapsing SELECT number, number * 10, 1, 1 FROM numbers(100);
INSERT INTO t_versioned_collapsing SELECT number, number * 10, -1, 1 FROM numbers(100);
INSERT INTO t_versioned_collapsing SELECT number, number * 100, 1, 2 FROM numbers(100);

SELECT 'versioned_collapsing_on';
SELECT key, value
FROM t_versioned_collapsing FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'versioned_collapsing_off';
SELECT key, value
FROM t_versioned_collapsing FINAL
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit must NOT appear
SELECT 'explain_versioned_collapsing_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, value
    FROM t_versioned_collapsing FINAL
    ORDER BY key ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Part 20: WHERE auto-moved to PREWHERE enables limit pushdown
-- With optimize_move_to_prewhere = 1 and optimize_move_to_prewhere_if_final = 1,
-- the WHERE clause is converted to PREWHERE automatically. This replaces
-- the FilterStep with an ExpressionStep, so has_filter_step stays false,
-- and limit pushdown is correctly enabled.
-- ============================================================

SELECT 'where_auto_prewhere_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT b, finalizeAggregation(val)
    FROM t_prewhere_asc FINAL
    WHERE a = 1
    ORDER BY b ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1,
             optimize_move_to_prewhere = 1,
             optimize_move_to_prewhere_if_final = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 21: ORDER BY extends beyond key — limit must NOT be pushed
-- Key (a, b), ORDER BY (a, b, c): the merge outputs rows sorted
-- by (a, b) but the SortingStep will re-sort by (a, b, c). Pushing
-- limit into the merge would discard rows that might rank higher
-- after the full re-sort.
--
-- Use SummingMergeTree so that non-key columns (c) remain
-- deterministic (summed). Each unique (a, b) group sums c and value.
-- ============================================================

DROP TABLE IF EXISTS t_order_extends_key;

CREATE TABLE t_order_extends_key
(
    a UInt32,
    b UInt32,
    c UInt32,
    value UInt64
)
ENGINE = SummingMergeTree
ORDER BY (a, b);

-- Make c inversely related to b within each a-group so that
-- ORDER BY (a, b) and ORDER BY (a, c) produce different orders.
-- a in {0,1,2}, b in {0..9}, c = (100 - b) * 10.
-- After FINAL: c is summed from both inserts = 2 * (100 - b) * 10.
INSERT INTO t_order_extends_key SELECT number % 3, number / 3, (100 - number / 3) * 10, 1 FROM numbers(30);
INSERT INTO t_order_extends_key SELECT number % 3, number / 3, (100 - number / 3) * 10, 2 FROM numbers(30);

-- Correctness: results must match with/without pushdown
SELECT 'order_extends_key_on';
SELECT a, b, c
FROM t_order_extends_key FINAL
ORDER BY a, b, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'order_extends_key_off';
SELECT a, b, c
FROM t_order_extends_key FINAL
ORDER BY a, b, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit must NOT appear (ORDER BY has 3 cols, key has 2)
SELECT 'explain_order_extends_key_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, b, c
    FROM t_order_extends_key FINAL
    ORDER BY a, b, c ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Part 22: ORDER BY diverges from key — limit must NOT be pushed
-- Key (a, b), ORDER BY (a, c): column c is not in the key at all.
-- The merge output order (a, b) differs from ORDER BY (a, c).
-- ============================================================

-- Correctness: results must match with/without pushdown
SELECT 'order_diverges_key_on';
SELECT a, c
FROM t_order_extends_key FINAL
ORDER BY a, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'order_diverges_key_off';
SELECT a, c
FROM t_order_extends_key FINAL
ORDER BY a, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit must NOT appear (c is not in the key)
SELECT 'explain_order_diverges_key_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, c
    FROM t_order_extends_key FINAL
    ORDER BY a, c ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Part 23: WHERE fixes middle key column — limit SHOULD be pushed
-- Key (a, b, c), PREWHERE b = 0, ORDER BY a, c: column b is fixed,
-- so the effective merge order is (a, c) which matches ORDER BY.
-- Limit pushdown is safe.
--
-- Use SummingMergeTree with key (a, b, c) so each row is unique.
-- ============================================================

DROP TABLE IF EXISTS t_where_fixes_middle;

CREATE TABLE t_where_fixes_middle
(
    a UInt32,
    b UInt32,
    c UInt32,
    value UInt64
)
ENGINE = SummingMergeTree
ORDER BY (a, b, c);

INSERT INTO t_where_fixes_middle SELECT number % 5, number % 3, number, 1 FROM numbers(300);
INSERT INTO t_where_fixes_middle SELECT number % 5, number % 3, number, 2 FROM numbers(300);

-- Correctness: results must match with/without pushdown
SELECT 'where_fixes_middle_on';
SELECT a, c, value
FROM t_where_fixes_middle FINAL
PREWHERE b = 0
ORDER BY a, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'where_fixes_middle_off';
SELECT a, c, value
FROM t_where_fixes_middle FINAL
PREWHERE b = 0
ORDER BY a, c ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear — b is fixed, (a, b=0, c) ≡ (a, c)
SELECT 'explain_where_fixes_middle_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, c, value
    FROM t_where_fixes_middle FINAL
    PREWHERE b = 0
    ORDER BY a, c ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 24: ORDER BY is a prefix of key — limit SHOULD be pushed
-- Key (a, b, c), ORDER BY (a, b): merge output is sorted by
-- (a, b, c) which refines (a, b). No re-sort needed.
-- ============================================================

-- Correctness: results must match with/without pushdown.
-- Don't select c — it's not in ORDER BY so its position is
-- non-deterministic within the same (a, b) group.
SELECT 'order_prefix_of_key_on';
SELECT a, b, value
FROM t_where_fixes_middle FINAL
ORDER BY a, b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'order_prefix_of_key_off';
SELECT a, b, value
FROM t_where_fixes_middle FINAL
ORDER BY a, b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear — ORDER BY (a, b) is covered by key (a, b, c)
SELECT 'explain_order_prefix_of_key_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, b, value
    FROM t_where_fixes_middle FINAL
    ORDER BY a, b ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 25: ORDER BY matches key exactly — limit SHOULD be pushed
-- Key (a, b), ORDER BY (a, b): exact match, baseline positive case.
-- ============================================================

SELECT 'order_exact_match_on';
SELECT a, b, value
FROM t_order_extends_key FINAL
ORDER BY a, b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'order_exact_match_off';
SELECT a, b, value
FROM t_order_extends_key FINAL
ORDER BY a, b ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit SHOULD appear — ORDER BY exactly matches key
SELECT 'explain_order_exact_match_limit';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, b, value
    FROM t_order_extends_key FINAL
    ORDER BY a, b ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 5%';

-- ============================================================
-- Part 26: ORDER BY a, intDiv(a, 10) — monotonic function of first
-- key column used as second ORDER BY column. The matching loop
-- matches 'a' to key[0] then tries 'intDiv(a,10)' against key[1]
-- ('b') — no match, loop breaks. sort_description_for_merging
-- has 1 entry ('a') while description has 2 — guard blocks pushdown.
-- Pushdown would be safe (intDiv(a,10) is determined by 'a', so
-- the re-sort is a no-op), but the matching loop doesn't backtrack
-- to re-match against already-consumed key columns.
-- ============================================================

SELECT 'order_by_monotonic_of_key_on';
SELECT a, intDiv(a, 10), value
FROM t_order_extends_key FINAL
ORDER BY a, intDiv(a, 10) ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1;

SELECT 'order_by_monotonic_of_key_off';
SELECT a, intDiv(a, 10), value
FROM t_order_extends_key FINAL
ORDER BY a, intDiv(a, 10) ASC
LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0;

-- EXPLAIN: limit must NOT appear (intDiv(a,10) doesn't match key[1])
SELECT 'explain_order_by_monotonic_no_limit';
SELECT count() = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, intDiv(a, 10), value
    FROM t_order_extends_key FINAL
    ORDER BY a, intDiv(a, 10) ASC
    LIMIT 5
    SETTINGS optimize_final_limit_pushdown = 1,
             optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE t_agg_final_limit;
DROP TABLE t_agg_final_limit_desc;
DROP TABLE t_agg_final_limit_part;
DROP TABLE t_small_agg;
DROP TABLE t_compound_key;
DROP TABLE t_replacing_final_limit;
DROP TABLE t_collapsing_final_limit;
DROP TABLE t_summing_final_limit;
DROP TABLE t_prewhere_asc;
DROP TABLE t_prewhere_desc;
DROP TABLE t_prewhere_where;
DROP TABLE t_coalescing_final_limit;
DROP TABLE t_versioned_collapsing;
DROP TABLE t_order_extends_key;
DROP TABLE t_where_fixes_middle;
