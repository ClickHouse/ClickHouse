-- Tags: no-parallel-replicas
-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, None); materialize_statistics_on_insert=(1, None); query_plan_convert_outer_join_to_inner_join=(1, None)

-- A comparison of a JSON path with a Bool literal is estimated at half of the rows, so the join builds the smaller side.

SET query_plan_join_swap_table = 'auto';

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t2 (k UInt64, j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t1 SELECT number FROM numbers(1000);
INSERT INTO t2 SELECT number, if(number % 10 = 0, '{"f":true}', '{"f":false}') FROM numbers(10000);

SELECT if(explain ILIKE '%.t1)%', 't1', 't2') FROM (EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = false) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT if(explain ILIKE '%.t1)%', 't1', 't2') FROM (EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = true) WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = false;
SELECT count() FROM t1 LEFT JOIN t2 ON t1.k = t2.k WHERE t2.j.f = true;

DROP TABLE t1;
DROP TABLE t2;
