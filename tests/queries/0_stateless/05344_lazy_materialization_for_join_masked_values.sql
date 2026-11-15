-- Lazy materialization over joins recomputes a value of the side a join can leave unmatched above the LIMIT as
-- `if(mask, value, default)`, so that the rows the join padded get the default, as the join itself stands there.
-- That holds for a constant of that side as well. `if` drops `LowCardinality`, so a masked value is cast back to its
-- type. Every query is followed by the columns read lazily, and the results are compared with the optimization
-- disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, s String, lc LowCardinality(String), heavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number FROM numbers(100);
-- Only the even keys are in `r`, so the odd ones are left unmatched.
INSERT INTO r SELECT number * 2, toString(number % 3), toString(number % 5), concat('r', toString(number)) FROM numbers(50);

SELECT '-- constants of the side the join can leave unmatched';
SELECT l.k, r.c, r.n, r.t, r.heavy FROM l LEFT JOIN (SELECT k, 'const' AS c, 42 AS n, toTypeName(lc) AS t, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, r.c, r.n, r.t, r.heavy FROM l LEFT JOIN (SELECT k, 'const' AS c, 42 AS n, toTypeName(lc) AS t, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT trim(explain) FROM (EXPLAIN SELECT l.k, r.c, r.n, r.t, r.heavy FROM l LEFT JOIN (SELECT k, 'const' AS c, 42 AS n, toTypeName(lc) AS t, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 1) WHERE explain LIKE '%Lazily read columns%';

SELECT '-- LowCardinality values, which keep their type';
SELECT l.k, r.a, toTypeName(r.a), r.b, toTypeName(r.b), r.heavy FROM l LEFT JOIN (SELECT k, identity(lc) AS a, lc = '1' AS b, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, r.a, toTypeName(r.a), r.b, toTypeName(r.b), r.heavy FROM l LEFT JOIN (SELECT k, identity(lc) AS a, lc = '1' AS b, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT trim(explain) FROM (EXPLAIN SELECT l.k, r.a, toTypeName(r.a), r.b, toTypeName(r.b), r.heavy FROM l LEFT JOIN (SELECT k, identity(lc) AS a, lc = '1' AS b, heavy FROM r) AS r ON l.k = r.k
ORDER BY l.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 1) WHERE explain LIKE '%Lazily read columns%';

DROP TABLE l;
DROP TABLE r;
