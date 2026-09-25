-- Lazy materialization for ORDER BY ... LIMIT over joins: the columns the result needs only for the rows
-- the LIMIT returns are read after it, by the row index of each table, which the joins pass through.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
-- Pinned so that both sides of a join are read directly, and the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, a UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (k UInt64, b UInt64, rheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

-- Two parts each, so that the row index spans parts.
INSERT INTO l SELECT number, number % 97, concat('l', toString(number)) FROM numbers(5000);
INSERT INTO l SELECT number, number % 97, concat('l', toString(number)) FROM numbers(5000, 5000);
INSERT INTO r SELECT number * 3, number % 89, concat('r', toString(number * 3)) FROM numbers(2000);
INSERT INTO r SELECT number * 3, number % 89, concat('r', toString(number * 3)) FROM numbers(2000, 2000);

-- Each query is followed by the number of lazy reads in its plan.

SELECT '-- inner';
SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 3;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 3);

SELECT '-- left, the right side stands at its defaults where nothing matched';
SELECT l.k, l.heavy, r.rheavy, r.b FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy, r.b FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5);

SELECT '-- right';
SELECT l.k, l.heavy, r.rheavy, r.b FROM l RIGHT JOIN r ON l.k = r.k + 1 WHERE r.b > 3 ORDER BY r.b, r.k LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy, r.b FROM l RIGHT JOIN r ON l.k = r.k + 1 WHERE r.b > 3 ORDER BY r.b, r.k LIMIT 5);

SELECT '-- full';
SELECT l.k, l.heavy, r.k, r.rheavy FROM l FULL JOIN r ON l.k = r.k + 20000 ORDER BY l.a, r.b, l.k, r.k, empty(r.rheavy) LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.k, r.rheavy FROM l FULL JOIN r ON l.k = r.k + 20000 ORDER BY l.a, r.b, l.k, r.k, empty(r.rheavy) LIMIT 5);

SELECT '-- one row of a side matches many of the other';
SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.a = r.b WHERE l.k < 300 AND r.k < 900 ORDER BY l.k, r.k LIMIT 6;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.a = r.b WHERE l.k < 300 AND r.k < 900 ORDER BY l.k, r.k LIMIT 6);

SELECT '-- three tables, a table joined twice';
SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l JOIN r ON l.k = r.k JOIN r AS r2 ON r2.k = l.k + 3 WHERE l.a > 10 ORDER BY l.a DESC, l.k LIMIT 4;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l JOIN r ON l.k = r.k JOIN r AS r2 ON r2.k = l.k + 3 WHERE l.a > 10 ORDER BY l.a DESC, l.k LIMIT 4);

SELECT '-- a value the filter uses is computed again after the LIMIT';
SELECT l.k, l.a + r.b AS s, l.heavy FROM l JOIN r ON l.k = r.k WHERE l.a + r.b > 150 ORDER BY l.k DESC LIMIT 3;

SELECT '-- a filter above the join guards an expression that would throw below it';
SELECT l.k, intDiv(1000, r.b) AS q, l.heavy FROM l JOIN r ON l.k = r.k WHERE r.b != 0 ORDER BY q, l.k LIMIT 3;

SELECT '-- the other side is not a table';
SELECT l.k, l.heavy, s.c FROM l JOIN (SELECT b, count() AS c FROM r GROUP BY b) AS s ON l.a = s.b ORDER BY l.a, l.k LIMIT 3;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, s.c FROM l JOIN (SELECT b, count() AS c FROM r GROUP BY b) AS s ON l.a = s.b ORDER BY l.a, l.k LIMIT 3);

SELECT '-- join_use_nulls: the values of the side the join can leave unmatched are recomputed under a mask';
SELECT l.k, l.heavy, concat(r.rheavy, '!'), r.b + 1 FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5 SETTINGS join_use_nulls = 1;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%'), countIf(explain LIKE '%Rows the joins matched%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, concat(r.rheavy, '!'), r.b + 1 FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5 SETTINGS join_use_nulls = 1);
SELECT l.k, l.heavy, r.k, r.rheavy FROM l FULL JOIN r ON l.k = r.k + 20000 ORDER BY l.a, r.b, l.k, r.k, empty(r.rheavy) LIMIT 5 SETTINGS join_use_nulls = 1;
SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l LEFT JOIN r ON l.k = r.k LEFT JOIN r AS r2 ON r2.k = r.k + 3 WHERE l.a > 10 ORDER BY l.a DESC, l.k LIMIT 6 SETTINGS join_use_nulls = 1;

SELECT '-- a value that can throw is not computed for the rows the join left unmatched';
SELECT l.k, l.heavy, x.rheavy, x.q FROM l LEFT JOIN (SELECT k, rheavy, intDiv(100, b) AS q FROM r WHERE b > 0) AS x ON l.k = x.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5;

SELECT '-- the build side of a join with a runtime filter is read lazily too';
SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 3 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%'), countIf(explain LIKE '%BuildRuntimeFilter%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 3 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);
SELECT l.k, l.heavy FROM l LEFT ANTI JOIN r ON l.k = r.k AND l.a = r.b WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 3 SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;

SELECT '-- the same results without the optimization';
SELECT l.k, l.heavy, concat(r.rheavy, '!'), r.b + 1 FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 0;
SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l LEFT JOIN r ON l.k = r.k LEFT JOIN r AS r2 ON r2.k = r.k + 3 WHERE l.a > 10 ORDER BY l.a DESC, l.k LIMIT 6 SETTINGS join_use_nulls = 1, query_plan_lazy_materialization_for_join = 0;
SELECT l.k, l.heavy, r.rheavy, r.b FROM l LEFT JOIN r ON l.k = r.k WHERE l.a > 5 ORDER BY l.a, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT l.k, l.heavy, r.k, r.rheavy FROM l FULL JOIN r ON l.k = r.k + 20000 ORDER BY l.a, r.b, l.k, r.k, empty(r.rheavy) LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.a = r.b WHERE l.k < 300 AND r.k < 900 ORDER BY l.k, r.k LIMIT 6 SETTINGS query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
