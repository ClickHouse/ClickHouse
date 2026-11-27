-- Lazy materialization over joins when a join below cannot prune its columns: a key name that occurs twice in its
-- input makes it keep them all. Such a join is a source for the optimization, so the other side of the join above it
-- is still read lazily. Every query is followed by the number of lazy reads in its plan, and the results are compared
-- with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE m (k UInt64, mheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(1000);
INSERT INTO m SELECT number * 3, concat('m', toString(number)) FROM numbers(400);
INSERT INTO r SELECT number * 2, concat('r', toString(number)) FROM numbers(600);

SELECT '-- the inner join is rejected, the right side of the outer join is read lazily';
SELECT x.k, x.heavy, r.rheavy FROM (SELECT a.k AS k, a.heavy AS heavy FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
ORDER BY x.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT x.k, x.heavy, r.rheavy FROM (SELECT a.k AS k, a.heavy AS heavy FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
ORDER BY x.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT x.k, x.heavy, r.rheavy FROM (SELECT a.k AS k, a.heavy AS heavy FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
    ORDER BY x.k DESC LIMIT 5);

SELECT '-- the rejected join is on the right side';
SELECT l.k, l.heavy, y.mheavy FROM l JOIN (SELECT b.k AS k, m.mheavy AS mheavy FROM (SELECT k, k FROM r) AS b JOIN m ON b.k = m.k) AS y ON l.k = y.k
ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, y.mheavy FROM l JOIN (SELECT b.k AS k, m.mheavy AS mheavy FROM (SELECT k, k FROM r) AS b JOIN m ON b.k = m.k) AS y ON l.k = y.k
ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.k, l.heavy, y.mheavy FROM l JOIN (SELECT b.k AS k, m.mheavy AS mheavy FROM (SELECT k, k FROM r) AS b JOIN m ON b.k = m.k) AS y ON l.k = y.k
    ORDER BY l.k DESC LIMIT 5);

SELECT '-- both joins are rejected, nothing is read lazily';
SELECT x.k, x.heavy, r.rheavy FROM (SELECT * FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
ORDER BY x.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT x.k, x.heavy, r.rheavy FROM (SELECT * FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
ORDER BY x.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT x.k, x.heavy, r.rheavy FROM (SELECT * FROM (SELECT k, k, heavy FROM l) AS a JOIN m ON a.k = m.k) AS x JOIN r ON x.k = r.k
    ORDER BY x.k DESC LIMIT 5);

DROP TABLE l;
DROP TABLE m;
DROP TABLE r;
