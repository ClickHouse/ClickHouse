-- Lazy materialization for LIMIT over joins without ORDER BY: the reads go through whole blocks before the LIMIT
-- stops them, and a hash join reads its build side completely, so the columns the result needs only for the rows
-- the LIMIT returns are read after it, by the row index of each table, the same way as with ORDER BY.
--
-- Without ORDER BY the LIMIT may return any rows, so every filter below keeps exactly as many rows as the LIMIT
-- does, and an ORDER BY above the LIMIT only sorts them for the output.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 20;
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
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.k BETWEEN 300 AND 314 LIMIT 5) ORDER BY k;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k WHERE l.k BETWEEN 300 AND 314 LIMIT 5);

SELECT '-- left, the right side stands at its defaults where nothing matched';
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy, r.b FROM l LEFT JOIN r ON l.k = r.k WHERE l.k BETWEEN 1000 AND 1009 LIMIT 10) ORDER BY k;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy, r.b FROM l LEFT JOIN r ON l.k = r.k WHERE l.k BETWEEN 1000 AND 1009 LIMIT 10);

SELECT '-- right';
SELECT * FROM (SELECT l.k, l.heavy, r.k, r.rheavy FROM l RIGHT JOIN r ON l.k = r.k + 1 WHERE r.k BETWEEN 3000 AND 3011 LIMIT 4) ORDER BY r.k;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.k, r.rheavy FROM l RIGHT JOIN r ON l.k = r.k + 1 WHERE r.k BETWEEN 3000 AND 3011 LIMIT 4);

SELECT '-- three tables, a table joined twice';
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l JOIN r ON l.k = r.k JOIN r AS r2 ON r2.k = l.k + 3 WHERE l.k BETWEEN 600 AND 611 LIMIT 4) ORDER BY 1;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN compact = 0 SELECT l.k, l.heavy, r.rheavy, r2.rheavy FROM l JOIN r ON l.k = r.k JOIN r AS r2 ON r2.k = l.k + 3 WHERE l.k BETWEEN 600 AND 611 LIMIT 4);

SELECT '-- a filter above the join that is not on the read';
SELECT * FROM (SELECT l.k, l.a + r.b AS s, l.heavy FROM l JOIN r ON l.k = r.k WHERE l.a + r.b > 150 AND l.k < 1000 LIMIT 18) ORDER BY k;

SELECT '-- a filter above the join guards an expression that would throw below it';
SELECT * FROM (SELECT l.k, intDiv(1000, r.b) AS q, l.heavy FROM l JOIN r ON l.k = r.k WHERE r.b != 0 AND l.k BETWEEN 3 AND 12 LIMIT 4) ORDER BY k;

SELECT '-- every row of the result is consistent with its keys, whichever rows the LIMIT returns';
SELECT count(), countIf(heavy = concat('l', toString(lk)) AND rheavy = concat('r', toString(rk)) AND lk = rk)
FROM (SELECT l.k AS lk, r.k AS rk, l.heavy AS heavy, r.rheavy AS rheavy FROM l JOIN r ON l.k = r.k LIMIT 20);
SELECT count(), countIf(heavy = concat('l', toString(lk)) AND if(rheavy = '', rk = 0, rheavy = concat('r', toString(rk)) AND lk = rk))
FROM (SELECT l.k AS lk, r.k AS rk, l.heavy AS heavy, r.rheavy AS rheavy FROM l LEFT JOIN r ON l.k = r.k LIMIT 20);

DROP TABLE l;
DROP TABLE r;
