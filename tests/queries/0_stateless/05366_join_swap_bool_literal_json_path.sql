-- Tags: no-parallel-replicas
-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, None); materialize_statistics_on_insert=(1, None); query_plan_convert_outer_join_to_inner_join=(1, None)

-- A comparison of a JSON path with a Bool literal is estimated at half of the rows, so the join builds the smaller side.

SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (k UInt64, j JSON) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t3 (k UInt64 STATISTICS(uniq), a Bool, b Bool STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t1 SELECT number FROM numbers(1000);
INSERT INTO t2 SELECT number, if(number % 10 = 0, '{"f":true}', '{"f":false}') FROM numbers(10000);
INSERT INTO t3 SELECT number, number % 10 = 0, number % 10 = 0 FROM numbers(10000);

SELECT if(explain ILIKE '%.t1)%', 't1', 't2') FROM (EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = false) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT if(explain ILIKE '%.t1)%', 't1', 't2') FROM (EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = true) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT extract(explain, 't2\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t2 ON t1.k = t2.k WHERE t2.j.f = false) WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT extract(explain, 't2\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t2 ON t1.k = t2.k WHERE t2.j.f != false) WHERE explain LIKE '%Join:%' LIMIT 1;

-- The same for a `Bool` column without statistics, a `Bool` column whose statistics cannot estimate an equality,
-- and a `Bool` expression in a set, each also above the set size limit.
SELECT extract(explain, 't3\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t3 ON t1.k = t3.k WHERE t3.a = true) WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT extract(explain, 't3\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t3 ON t1.k = t3.k WHERE t3.b = true) WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT extract(explain, 't3\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t3 ON t1.k = t3.k WHERE t3.a IN (false, true) SETTINGS statistics_max_set_size_for_exact_selectivity_estimation = 1) WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT extract(explain, 't3\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t3 ON t1.k = t3.k WHERE toBool(t3.k % 2) IN (false, true)) WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT extract(explain, 't3\\[(\\d+)\\]') FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t1 INNER JOIN t3 ON t1.k = t3.k WHERE toBool(t3.k % 2) IN (false, true) SETTINGS statistics_max_set_size_for_exact_selectivity_estimation = 1) WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = false;
SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = true;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
