-- `(t1 LEFT JOIN t2) LEFT JOIN t3 ON t2.k = t3.k` may be reassociated into `t1 LEFT JOIN (t2 LEFT JOIN t3)`
-- only when the second ON clause rejects the rows `t1 LEFT JOIN t2` extends with NULL. With a `Dynamic`
-- `t3.k` the keys are compared as `Dynamic`, where a NULL key matches a NULL key, so it does not:
-- reordering under a conflict detector must keep those matches.
-- https://github.com/ClickHouse/ClickHouse/issues/122152

SET enable_analyzer = 1;
SET join_use_nulls = 1;
SET allow_dynamic_type_in_join_keys = 1;
SET query_plan_optimize_join_order_limit = 10; -- the harness randomizes it, and at 0 nothing is reordered
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;

DROP TABLE IF EXISTS t1_05296;
DROP TABLE IF EXISTS t2_05296;
DROP TABLE IF EXISTS t3_dynamic_05296;

CREATE TABLE t1_05296 (a Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2_05296 (a Int64, k Nullable(Int64)) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t3_dynamic_05296 (k Dynamic, v String) ENGINE = MergeTree ORDER BY tuple();

-- Half of `t2` has a NULL key, and half of `t1` has no `t2` row at all.
INSERT INTO t1_05296 SELECT number FROM numbers(1000);
INSERT INTO t2_05296 SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(500);
INSERT INTO t3_dynamic_05296 SELECT NULL::Nullable(Int64), 'null key';
INSERT INTO t3_dynamic_05296 SELECT number::Int64, 'key' FROM numbers(1000);

SELECT 'Dynamic key';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_conflict_detector = '';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'a';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'c';

SELECT 'Dynamic key, compared with an expression over the Nullable key';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k + 0 = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_conflict_detector = '';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k + 0 = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'a';
SELECT count(), countIf(t3.v = 'null key') FROM t1_05296 AS t1 LEFT JOIN t2_05296 AS t2 ON t1.a = t2.a LEFT JOIN t3_dynamic_05296 AS t3 ON t2.k + 0 = t3.k
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'c';

DROP TABLE t1_05296;
DROP TABLE t2_05296;
DROP TABLE t3_dynamic_05296;
