-- Lazy FINAL with a WHERE that stays a `Filter` step above the read, when lazy materialization over joins puts a
-- step computing the row index between them: lazy FINAL still finds the filter and replaces the read.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET query_plan_optimize_lazy_final = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;
-- The filter is not moved to PREWHERE.
SET optimize_move_to_prewhere_if_final = 0;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;

CREATE TABLE f (k UInt64, v UInt64, a UInt64, heavy String) ENGINE = ReplacingMergeTree(v) ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE d (k UInt64, b UInt64, dheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;

-- Two parts that intersect, and one that intersects neither.
SYSTEM STOP MERGES f;
INSERT INTO f SELECT number, 1, number % 97, concat('v1_', toString(number)) FROM numbers(10000);
INSERT INTO f SELECT number * 2, 2, (number * 2) % 89, concat('v2_', toString(number * 2)) FROM numbers(5000);
INSERT INTO f SELECT number + 30000, 1, number % 97, concat('v1_', toString(number + 30000)) FROM numbers(10000);
INSERT INTO d SELECT number, number % 7, concat('d', toString(number)) FROM numbers(40000);

-- Each query is followed by whether the read is lazy, and whether lazy FINAL replaced it.

SELECT '-- a filter on the key';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') >= 2, countIf(explain LIKE '%InputSelector%') FROM (EXPLAIN SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5);

SELECT '-- a filter on the version';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.v = 1 ORDER BY f.a DESC, f.k LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') >= 2, countIf(explain LIKE '%InputSelector%') FROM (EXPLAIN SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.v = 1 ORDER BY f.a DESC, f.k LIMIT 5);

SELECT '-- the right side of a LEFT JOIN stands at its defaults where nothing matched';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL LEFT JOIN d ON f.k + 38000 = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') >= 2, countIf(explain LIKE '%InputSelector%') FROM (EXPLAIN SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL LEFT JOIN d ON f.k + 38000 = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5);

SELECT '-- the same results without lazy FINAL';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.v = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL LEFT JOIN d ON f.k + 38000 = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0;

DROP TABLE f;
DROP TABLE d;
