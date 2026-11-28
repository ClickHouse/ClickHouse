-- Lazy materialization over a join reads the lazy columns by the row index of each table. A row that matches several
-- rows of the other side has its index several times, and the lazy read reads it once and repeats it.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(10);
-- Keys 2 and 4 match three and two rows of `r`, key 7 one row.
INSERT INTO r VALUES (2, 'r2a'), (2, 'r2b'), (2, 'r2c'), (4, 'r4a'), (4, 'r4b'), (7, 'r7');

SELECT '-- without ORDER BY';
SELECT * FROM (SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k LIMIT 10) ORDER BY ALL;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k LIMIT 10);

SELECT '-- with ORDER BY';
SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k ORDER BY r.rheavy LIMIT 10;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') FROM (EXPLAIN SELECT l.k, l.heavy, r.rheavy FROM l JOIN r ON l.k = r.k ORDER BY r.rheavy LIMIT 10);

DROP TABLE l;
DROP TABLE r;
