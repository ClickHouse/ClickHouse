-- `optimize_rewrite_sum_if_to_count_if` rewrites `sum(if(cond, 0, 1))` into `countIf(not(cond))`.
-- That rewrite is only valid when the condition cannot be NULL: `if` takes the else branch on a NULL
-- condition and the row counts, while `not(NULL)` is NULL and `countIf` skips it.
-- The guard used to ask the condition type `isNullable`, which is false for a `LowCardinality` wrapper,
-- so the rewrite fired for `LowCardinality(Nullable(...))` conditions and undercounted.

SET enable_analyzer = 1;
SET optimize_rewrite_sum_if_to_count_if = 1;
-- Isolate this pass: `optimize_rewrite_aggregate_function_with_if` can reach the same rewrite by another route.
SET optimize_rewrite_aggregate_function_with_if = 0;

DROP TABLE IF EXISTS t_sum_if_lc_nullable;

CREATE TABLE t_sum_if_lc_nullable
(
    s LowCardinality(Nullable(String)),
    n Nullable(UInt8),
    u UInt8
)
ENGINE = Memory;

INSERT INTO t_sum_if_lc_nullable VALUES ('y', 1, 1), ('n', 0, 0), (NULL, NULL, 0);

-- A comparison over a LowCardinality(Nullable(...)) column keeps the LowCardinality wrapper.
SELECT DISTINCT toTypeName(s = 'y') FROM t_sum_if_lc_nullable;

SELECT '-- LowCardinality(Nullable) condition: rewrite must not fire';
-- Both 'n' and NULL take the else branch, so the answer is 2, and 10 for the multiplier variant.
SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_lc_nullable;
SELECT sum(if(s = 'y', 0, 5)) FROM t_sum_if_lc_nullable;
SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;
SELECT sum(if(s = 'y', 0, 5)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(s = 'y', 0, 1)) FROM t_sum_if_lc_nullable) WHERE explain ILIKE '%countIf%';

SELECT '-- Nullable condition: rewrite must not fire';
SELECT sum(if(n = 1, 0, 1)) FROM t_sum_if_lc_nullable;
SELECT sum(if(n = 1, 0, 1)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(n = 1, 0, 1)) FROM t_sum_if_lc_nullable) WHERE explain ILIKE '%countIf%';

SELECT '-- Non-nullable condition: rewrite must still fire';
SELECT sum(if(u = 1, 0, 1)) FROM t_sum_if_lc_nullable;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(u = 1, 0, 1)) FROM t_sum_if_lc_nullable) WHERE explain ILIKE '%countIf%';

SELECT '-- The un-negated direction agrees with `countIf` even for NULL conditions and must still be rewritten';
SELECT sum(if(s = 'y', 1, 0)) FROM t_sum_if_lc_nullable;
SELECT sum(if(s = 'y', 1, 0)) FROM t_sum_if_lc_nullable SETTINGS optimize_rewrite_sum_if_to_count_if = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(s = 'y', 1, 0)) FROM t_sum_if_lc_nullable) WHERE explain ILIKE '%countIf%';

DROP TABLE t_sum_if_lc_nullable;
