-- Lazy materialization over joins of a MergeTree table with a Memory table. The Memory table cannot be read lazily,
-- so its columns stay in the main read, while the columns of the MergeTree table are read lazily, whichever side it
-- is on. Every query is followed by the number of lazy reads in its plan, and the results are compared with the
-- optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS mem;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE mem (k UInt64, mheavy String) ENGINE = Memory;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(1000);
INSERT INTO r SELECT number * 2, concat('r', toString(number)) FROM numbers(600);
INSERT INTO mem SELECT number * 3, concat('m', toString(number)) FROM numbers(400);

SELECT '-- the Memory table on the right';
SELECT l.k, l.heavy, mem.mheavy FROM l JOIN mem ON l.k = mem.k ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, mem.mheavy FROM l JOIN mem ON l.k = mem.k ORDER BY l.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, mem.mheavy FROM l JOIN mem ON l.k = mem.k ORDER BY l.k DESC LIMIT 5);

SELECT '-- the Memory table on the left';
SELECT mem.k, mem.mheavy, l.heavy FROM mem JOIN l ON mem.k = l.k ORDER BY mem.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT mem.k, mem.mheavy, l.heavy FROM mem JOIN l ON mem.k = l.k ORDER BY mem.k DESC LIMIT 5 SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT mem.k, mem.mheavy, l.heavy FROM mem JOIN l ON mem.k = l.k ORDER BY mem.k DESC LIMIT 5);

SELECT '-- three tables, the Memory table in the middle';
SELECT l.k, l.heavy, mem.mheavy, r.rheavy FROM l JOIN mem ON l.k = mem.k JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, mem.mheavy, r.rheavy FROM l JOIN mem ON l.k = mem.k JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (
    EXPLAIN SELECT l.k, l.heavy, mem.mheavy, r.rheavy FROM l JOIN mem ON l.k = mem.k JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 5);

DROP TABLE l;
DROP TABLE r;
DROP TABLE mem;
