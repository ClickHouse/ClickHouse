-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/89802
-- MergeTreeIndexConditionSet cloned the predicate onto the granule block verbatim, keeping
-- functions pre-resolved against a Nullable type while the granule column carries the plain
-- storage type. Executing the actions then hit:
--   Logical error: 'Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8...'
-- Two independent ways to make the predicate Nullable are covered: a query-side toNullable()
-- wrapper on a set-index expression, and a MaterializedView declaring the storage column Nullable.

SET enable_analyzer = 1;

-- 1) Query-side Nullable: `t % toNullable(19) = c` resolves equals to Nullable(UInt8) while the
-- `set` index granule computes `t % 19` at plain UInt8 (found by the AST fuzzer on 01786).
DROP TABLE IF EXISTS t_04538;
CREATE TABLE t_04538 (t UInt32, INDEX t_set t % 19 TYPE set(4) GRANULARITY 2)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_04538 SELECT number FROM numbers(20);

SELECT 'qs_match', count() FROM t_04538 WHERE (t % toNullable(19)) = 2;
SELECT 'qs_no_match', count() FROM t_04538 WHERE (t % toNullable(19)) = -2;
SELECT 'qs_and', count() FROM t_04538 WHERE (t % toNullable(19)) = 2 AND t > 0;
SELECT 'qs_or', count() FROM t_04538 WHERE (t % toNullable(19)) = 2 OR (t % toNullable(19)) = 3;
SELECT 'qs_not', count() FROM t_04538 WHERE NOT ((t % toNullable(19)) = 100);

-- The set index must still prune: the t_set index reads 0 granules for `t % 19 = -2`.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_04538 WHERE (t % toNullable(19)) = -2
) WHERE explain LIKE '%Granules: 0/%';

-- 1b) Mixed plain and Nullable uses of the SAME set-index expression in one predicate.
-- `t % 19` (plain UInt8) and `t % toNullable(19)` (Nullable(UInt8)) canonicalize to the same
-- column name `modulo(t, 19)`, so the DAG rebuild must keep a separately-typed adapted node per
-- occurrence — a name-only cache would reuse one branch's node for the other and re-trigger the
-- `Unexpected return type from equals` logical error. Both AND/OR orders are exercised.
SELECT 'mix_or', count() FROM t_04538 WHERE (t % 19) = 1 OR (t % toNullable(19)) = 2;
SELECT 'mix_or_rev', count() FROM t_04538 WHERE (t % toNullable(19)) = 2 OR (t % 19) = 1;
SELECT 'mix_and', count() FROM t_04538 WHERE (t % 19) = 1 AND (t % toNullable(19)) = 1;

-- 2) MaterializedView-declared Nullable column over a non-Nullable set-index storage column,
-- with query_plan_merge_expressions = 0 keeping the MV's _CAST separate from the WHERE filter.
DROP TABLE IF EXISTS t_mv_04538;
DROP TABLE IF EXISTS v_mv_04538;
CREATE TABLE t_mv_04538 (c0 Int, INDEX i0 c0 TYPE set(0)) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v_mv_04538 TO t_mv_04538 (c0 Nullable(Int)) AS (SELECT 1 c0);
INSERT INTO TABLE t_mv_04538 (c0) VALUES (1), (2), (3), (10);

SELECT 'mv_no_match', count() FROM v_mv_04538 WHERE c0 = 0 SETTINGS query_plan_merge_expressions = 0;
SELECT 'mv_match', count() FROM v_mv_04538 WHERE c0 = 1 SETTINGS query_plan_merge_expressions = 0;
SELECT 'mv_in', count() FROM v_mv_04538 WHERE c0 IN (1, 2, 100) SETTINGS query_plan_merge_expressions = 0;

DROP TABLE v_mv_04538;
DROP TABLE t_mv_04538;
DROP TABLE t_04538;
