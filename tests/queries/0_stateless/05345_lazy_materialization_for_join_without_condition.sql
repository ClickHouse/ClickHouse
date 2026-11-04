-- Lazy materialization over joins with joins that have no condition reading a side: CROSS JOIN, a comma join, ON 1,
-- and PASTE JOIN. Such a join keeps the first column of a side nothing else of it is needed from, so that column stays
-- in the main read and crosses the LIMIT, while the other columns are read lazily. Every query is followed by the
-- number of lazy reads in its plan, and the results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in, and a comma join stays a cross join.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET cross_to_inner_join_rewrite = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

-- The heavy columns come first, so that they are the ones a join keeps of a side when nothing else is needed of it.
CREATE TABLE l (heavy String, k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (rheavy String, k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

INSERT INTO l SELECT concat('l', toString(number)), number FROM numbers(1000);
INSERT INTO r SELECT concat('r', toString(number)), number FROM numbers(30);

SELECT '-- CROSS JOIN';
SELECT l.k, l.heavy, r.k, r.rheavy FROM l CROSS JOIN r ORDER BY l.k DESC, r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, r.k, r.rheavy FROM l CROSS JOIN r ORDER BY l.k DESC, r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, r.k, r.rheavy FROM l CROSS JOIN r ORDER BY l.k DESC, r.k LIMIT 5);

SELECT '-- a comma join, only the heavy columns in the result';
SELECT l.heavy, r.rheavy FROM l, r ORDER BY l.k, r.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.heavy, r.rheavy FROM l, r ORDER BY l.k, r.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.heavy, r.rheavy FROM l, r ORDER BY l.k, r.k DESC LIMIT 5);

SELECT '-- LEFT JOIN ON 1';
SELECT l.k, l.heavy, r.rheavy FROM l LEFT JOIN r ON 1 ORDER BY l.k DESC, r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, r.rheavy FROM l LEFT JOIN r ON 1 ORDER BY l.k DESC, r.k LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, r.rheavy FROM l LEFT JOIN r ON 1 ORDER BY l.k DESC, r.k LIMIT 5);

SELECT '-- nothing of the right side is needed below the LIMIT, so the join keeps its heavy column';
SELECT l.heavy FROM l CROSS JOIN r ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.heavy FROM l CROSS JOIN r ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.heavy FROM l CROSS JOIN r ORDER BY l.k DESC LIMIT 5);

SELECT '-- PASTE JOIN';
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy FROM (SELECT * FROM l ORDER BY k LIMIT 30) AS l PASTE JOIN (SELECT * FROM r ORDER BY k) AS r) ORDER BY k LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy FROM (SELECT * FROM l ORDER BY k LIMIT 30) AS l PASTE JOIN (SELECT * FROM r ORDER BY k) AS r) ORDER BY k LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
