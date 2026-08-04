-- Regression tests for RewriteOrderByLimitPass (setting query_plan_rewrite_order_by_limit).
-- The rewrite replaces `ORDER BY ... LIMIT` over a MergeTree table with a physical-offset filter,
-- so the subquery it injects carries a projection column named `_cumulative_part_offset`; counting
-- those tells whether (and how many times) the rewrite fired.

DROP TABLE IF EXISTS t_rewrite_order_by_limit;
CREATE TABLE t_rewrite_order_by_limit (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_rewrite_order_by_limit SELECT number, number * 10 FROM numbers(100);

SET enable_analyzer = 1;
SET query_plan_rewrite_order_by_limit = 1;
-- The default threshold is 50 projected columns; lower it so a two-column projection is eligible.
SET query_plan_min_columns_to_use_rewrite_order_by_limit = 2;

SELECT '-- a deterministic ORDER BY is rewritten (control)';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a LIMIT 2);

SELECT '-- a non-deterministic ORDER BY (rand) must NOT be rewritten (it would be evaluated twice)';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit ORDER BY rand() LIMIT 2);

SELECT '-- rand nested in the sort expression is also rejected';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a + rand() LIMIT 2);

SELECT '-- rand in a secondary sort key is also rejected';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a, rand() LIMIT 2);

SELECT '-- a function that is constant within the query (now) is still rewritten';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a + toUInt64(now()) LIMIT 2);

SELECT '-- both arms of a UNION ALL are rewritten (traversal must not stop after the first)';
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a LIMIT 2
    UNION ALL
    SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a DESC LIMIT 3);

SELECT '-- the UNION ALL result is unchanged by the rewrite';
SELECT a, b FROM (
    SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a LIMIT 2
    UNION ALL
    SELECT a, b FROM t_rewrite_order_by_limit ORDER BY a DESC LIMIT 3
) ORDER BY a, b;

SELECT '-- a table with a normal projection must NOT be rewritten (projection reads use projection-local part offsets)';
DROP TABLE IF EXISTS t_rewrite_order_by_limit_proj;
CREATE TABLE t_rewrite_order_by_limit_proj (a UInt64, b UInt64, PROJECTION p_b (SELECT a, b ORDER BY b)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_rewrite_order_by_limit_proj SELECT number, number * 10 FROM numbers(100);
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit_proj ORDER BY b LIMIT 2);

SELECT '-- an aggregate projection does not block the rewrite (it cannot serve a non-aggregate read)';
DROP TABLE IF EXISTS t_rewrite_order_by_limit_agg_proj;
CREATE TABLE t_rewrite_order_by_limit_agg_proj (a UInt64, b UInt64, PROJECTION p_sum (SELECT a, sum(b) GROUP BY a)) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_rewrite_order_by_limit_agg_proj SELECT number, number * 10 FROM numbers(100);
SELECT countIf(explain LIKE '%_cumulative_part_offset%')
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT a, b FROM t_rewrite_order_by_limit_agg_proj ORDER BY b LIMIT 2);

DROP TABLE t_rewrite_order_by_limit_proj;
DROP TABLE t_rewrite_order_by_limit_agg_proj;
DROP TABLE t_rewrite_order_by_limit;
