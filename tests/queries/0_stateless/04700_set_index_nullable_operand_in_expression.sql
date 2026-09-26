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
-- settings and assert that the index is in the plan rather than inferring it from the row counts.
-- `use_skip_indexes_on_data_read` and `use_query_condition_cache` move pruning out of index
-- analysis, which is where `EXPLAIN indexes = 1` reports the granule counts asserted below.
SET use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0;

SELECT 'index_used', countIf(explain LIKE '%Name: t_set%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % 19 = 16);

-- Pruning must be unchanged, not merely correct: an atom that falls back to `UNKNOWN_FIELD` leaves
-- every granule unpruned and still counts 5 rows, so assert the counts match the plain expression.
SELECT 'granules_plain', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % 19 = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'granules_to_nullable', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % toNullable(19) = 16)
WHERE explain LIKE '%Granules: %/%';

-- `nullIf(19, 0)` folds to a constant before index analysis, so it renders as `modulo(t, 19)` and
-- matches the key column. Assert that, since the shape reads as if the `nullIf` node survived.
SELECT 'granules_null_if', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % nullIf(19, 0) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'granules_subquery', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % (SELECT 19) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'plain', count() FROM t_113234 WHERE t % 19 = 16;
SELECT 'to_nullable', count() FROM t_113234 WHERE t % toNullable(19) = 16;
SELECT 'null_if', count() FROM t_113234 WHERE t % nullIf(19, 0) = 16;
SELECT 'scalar_subquery', count() FROM t_113234 WHERE t % (SELECT toNullable(19)) = 16;

-- A scalar subquery is typed `Nullable` because it may return no rows, so it reaches the same
-- collision with no `Nullable` written anywhere in the query.
SELECT 'subquery_plain', count() FROM t_113234 WHERE t % (SELECT 19) = 16;

-- A `Nullable` comparison constant sits outside the indexed expression and was never affected.
SELECT 'nullable_constant', count() FROM t_113234 WHERE t % 19 = toNullable(16);

-- Without bulk filtering the condition is evaluated through `mayBeTrueOnGranule`, which runs the
-- hyperrectangle check before the same actions, so cover that path too.
SET secondary_indices_enable_bulk_filtering = 0;

SELECT 'nobulk_index_used', countIf(explain LIKE '%Name: t_set%') > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % 19 = 16);

SELECT 'nobulk_granules_plain', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % 19 = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'nobulk_granules_to_nullable', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % toNullable(19) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'nobulk_granules_null_if', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % nullIf(19, 0) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'nobulk_granules_subquery', extract(explain, 'Granules: [0-9]+/[0-9]+')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_113234 WHERE t % (SELECT 19) = 16)
WHERE explain LIKE '%Granules: %/%';

SELECT 'nobulk_plain', count() FROM t_113234 WHERE t % 19 = 16;
SELECT 'nobulk_to_nullable', count() FROM t_113234 WHERE t % toNullable(19) = 16;
SELECT 'nobulk_null_if', count() FROM t_113234 WHERE t % nullIf(19, 0) = 16;
SELECT 'nobulk_subquery_plain', count() FROM t_113234 WHERE t % (SELECT 19) = 16;
SELECT 'nobulk_nullable_constant', count() FROM t_113234 WHERE t % 19 = toNullable(16);

DROP TABLE t_113234;
