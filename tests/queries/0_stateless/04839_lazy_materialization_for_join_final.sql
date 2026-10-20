-- Lazy materialization over joins with a ReplacingMergeTree read with FINAL: the columns FINAL merges on
-- stay in the main read, the rest of the row FINAL keeps is read after the LIMIT. Also together with lazy
-- FINAL, which replaces the same read by a set-based plan.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;
-- Lazy FINAL needs the filter in PREWHERE.
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;

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

-- Each query is followed by the number of lazy reads in its plan, and whether lazy FINAL replaced the read.

SELECT '-- FINAL keeps the newest version, and the rest of its row is read after the LIMIT';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%'), countIf(explain LIKE '%InputSelector%') FROM (EXPLAIN SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0);

SELECT '-- the same with lazy FINAL';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 1;
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%') >= 3, countIf(explain LIKE '%InputSelector%') FROM (EXPLAIN SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 1);

SELECT '-- the FINAL table on the side a join can leave unmatched';
SELECT f.k, f.v, f.heavy, d.dheavy FROM d LEFT JOIN f FINAL ON f.k = d.k WHERE d.b = 2 AND f.k % 3 != 0 ORDER BY f.v DESC, f.k, d.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 1;
SELECT f.k, f.v, f.heavy, d.dheavy FROM d LEFT JOIN f FINAL ON f.k = d.k WHERE d.b = 2 AND d.k >= 29980 ORDER BY d.k LIMIT 6 SETTINGS query_plan_optimize_lazy_final = 1, join_use_nulls = 1;

SELECT '-- the same results without either optimization';
SELECT f.k, f.v, f.heavy, d.dheavy FROM f FINAL JOIN d ON f.k = d.k WHERE f.k % 5 = 1 ORDER BY f.a DESC, f.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0, query_plan_lazy_materialization_for_join = 0;
SELECT f.k, f.v, f.heavy, d.dheavy FROM d LEFT JOIN f FINAL ON f.k = d.k WHERE d.b = 2 AND f.k % 3 != 0 ORDER BY f.v DESC, f.k, d.k LIMIT 5 SETTINGS query_plan_optimize_lazy_final = 0, query_plan_lazy_materialization_for_join = 0;
SELECT f.k, f.v, f.heavy, d.dheavy FROM d LEFT JOIN f FINAL ON f.k = d.k WHERE d.b = 2 AND d.k >= 29980 ORDER BY d.k LIMIT 6 SETTINGS query_plan_optimize_lazy_final = 0, query_plan_lazy_materialization_for_join = 0, join_use_nulls = 1;

DROP TABLE f;
DROP TABLE d;
