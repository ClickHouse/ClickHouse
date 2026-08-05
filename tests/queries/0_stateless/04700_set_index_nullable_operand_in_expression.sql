-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/113234
-- A `set` skip index defined on an expression used to raise
--   Logical error: 'Unexpected return type from equals. Expected Nullable(UInt8). Got UInt8'
-- when the query's copy of the indexed expression carried a `Nullable` operand. Such an operand
-- folds to the same name as the index expression, so `MergeTreeIndexConditionSet` matched it by
-- name and replaced the whole subtree with the granule column, dropping the operand while the
-- reused function kept declaring a `Nullable` return type.
-- `index_granularity = 2` keeps every granule within the `set(4)` capacity, otherwise the granule
-- stores no set elements and the condition is never evaluated.

DROP TABLE IF EXISTS t_113234;

CREATE TABLE t_113234 (t UInt32, INDEX t_set t % 19 TYPE set(4) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;

INSERT INTO t_113234 SELECT number FROM numbers(100);

-- The queries below only cover the fix while the skip index is actually consulted, so pin the
-- setting and assert that the index is in the plan rather than inferring it from the row counts.
SET use_skip_indexes = 1;

SELECT 'index_used', countIf(explain LIKE '%Name: t_set%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % 19 = 16);

SELECT 'plain', count() FROM t_113234 WHERE t % 19 = 16;
SELECT 'to_nullable', count() FROM t_113234 WHERE t % toNullable(19) = 16;
SELECT 'null_if', count() FROM t_113234 WHERE t % nullIf(19, 0) = 16;
SELECT 'scalar_subquery', count() FROM t_113234 WHERE t % (SELECT toNullable(19)) = 16;
SELECT 'materialize', count() FROM t_113234 WHERE t % materialize(toNullable(19)) = 16;

-- A scalar subquery is typed `Nullable` because it may return no rows, so parameterising the
-- indexed expression from a lookup table reaches the same collision with no `Nullable` anywhere
-- in the schema or the query text.
DROP TABLE IF EXISTS cfg_113234;
CREATE TABLE cfg_113234 (divisor UInt32) ENGINE = Memory;
INSERT INTO cfg_113234 VALUES (19);

SELECT 'subquery_from_table', count() FROM t_113234 WHERE t % (SELECT divisor FROM cfg_113234) = 16;

-- A `Nullable` comparison constant sits outside the indexed expression and was never affected.
SELECT 'nullable_constant', count() FROM t_113234 WHERE t % 19 = toNullable(16);

DROP TABLE cfg_113234;
DROP TABLE t_113234;
