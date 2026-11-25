-- https://github.com/ClickHouse/ClickHouse/issues/116937
-- `optimize_rewrite_aggregate_function_with_if` rewrites `agg(if(cond, const, x))` - the form with
-- the constant in the first branch - into `aggIf(x, not(cond))`. For a condition that can be NULL
-- that is not equivalent: `if` sends a NULL condition down the else branch, so the row contributes
-- `x`, while `not(NULL)` is NULL and `aggIf` skips the row.

SET optimize_rewrite_aggregate_function_with_if = 1, optimize_rewrite_sum_if_to_count_if = 0;

DROP TABLE IF EXISTS t_agg_if_nullable_cond;
CREATE TABLE t_agg_if_nullable_cond (c Nullable(UInt8), x Int64) ENGINE = Memory;
INSERT INTO t_agg_if_nullable_cond VALUES (1, 100), (0, 10), (NULL, 7);

SELECT sum(if(c, 0, x)) FROM t_agg_if_nullable_cond;
SELECT sum(if(c, 0, x)) FROM t_agg_if_nullable_cond SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

SELECT avg(if(c, NULL, x)) FROM t_agg_if_nullable_cond;
SELECT avg(if(c, NULL, x)) FROM t_agg_if_nullable_cond SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

SELECT count(if(c, NULL, x)) FROM t_agg_if_nullable_cond;
SELECT count(if(c, NULL, x)) FROM t_agg_if_nullable_cond SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

-- A `LowCardinality(Nullable(...))` condition is refused for the same reason.
SELECT sum(if(toLowCardinality(c), 0, x)) FROM t_agg_if_nullable_cond;
SELECT sum(if(toLowCardinality(c), 0, x)) FROM t_agg_if_nullable_cond SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

-- The un-negated direction was always correct.
SELECT 'un-negated';
SELECT sum(if(c, x, 0)), avg(if(c, x, NULL)) FROM t_agg_if_nullable_cond;
SELECT sum(if(c, x, 0)), avg(if(c, x, NULL)) FROM t_agg_if_nullable_cond SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

-- The rewrite still fires for a condition that cannot be NULL.
SELECT 'not nullable';
DROP TABLE IF EXISTS t_agg_if_plain_cond;
CREATE TABLE t_agg_if_plain_cond (c UInt8, x Int64) ENGINE = Memory;
INSERT INTO t_agg_if_plain_cond VALUES (1, 100), (0, 10);
SELECT sum(if(c, 0, x)) FROM t_agg_if_plain_cond;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT sum(if(c, 0, x)) FROM t_agg_if_plain_cond) WHERE explain LIKE '%sumIf%';

DROP TABLE t_agg_if_nullable_cond;
DROP TABLE t_agg_if_plain_cond;
