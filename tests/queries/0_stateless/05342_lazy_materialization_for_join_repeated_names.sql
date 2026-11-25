-- Lazy materialization over joins matches the columns of the steps it rebuilds by position, so a name that occurs twice
-- in a header does not confuse it. A join whose condition reads a name that occurs twice does not prune its columns,
-- and the optimization does not apply there. Every query is followed by the number of lazy reads in its plan, and the
-- results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, a UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (k UInt64, b UInt64, rheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

INSERT INTO l SELECT number, number % 97, concat('l', toString(number)) FROM numbers(10000);
INSERT INTO r SELECT number * 3, number % 89, concat('r', toString(number * 3)) FROM numbers(4000);

SELECT '-- the join key twice: the join does not prune, so nothing is read lazily';
SELECT * FROM (SELECT k, k, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT * FROM (SELECT k, k, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT * FROM (SELECT k, k, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1);

SELECT '-- another column twice, on either side';
SELECT * FROM (SELECT k, a, a, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT * FROM (SELECT k, a, a, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT * FROM (SELECT k, a, a, heavy FROM l) AS l JOIN r ON l.k = r.k ORDER BY r.b, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1);
SELECT l.k, l.heavy, r.rheavy, r.b FROM l JOIN (SELECT k, b, b, rheavy FROM r) AS r ON l.k = r.k ORDER BY l.a, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, r.rheavy, r.b FROM l JOIN (SELECT k, b, b, rheavy FROM r) AS r ON l.k = r.k ORDER BY l.a, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, r.rheavy, r.b FROM l JOIN (SELECT k, b, b, rheavy FROM r) AS r ON l.k = r.k ORDER BY l.a, l.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1);

SELECT '-- an expression twice, without ORDER BY, with a filter that keeps as many rows as the LIMIT';
SELECT * FROM (SELECT * FROM (SELECT k, a + 1, a + 1, heavy FROM l WHERE k BETWEEN 300 AND 312) AS l JOIN r ON l.k = r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1) ORDER BY ALL;
SELECT * FROM (SELECT * FROM (SELECT k, a + 1, a + 1, heavy FROM l WHERE k BETWEEN 300 AND 312) AS l JOIN r ON l.k = r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0) ORDER BY ALL;

DROP TABLE l;
DROP TABLE r;
