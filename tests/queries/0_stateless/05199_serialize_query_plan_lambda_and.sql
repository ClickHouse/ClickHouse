-- A lambda body becomes `ExpressionActions` while the query plan is built, so with
-- `serialize_query_plan = 1` that very DAG is what is shipped to the shard. A JIT-compiled node in it
-- has no name in `FunctionFactory` - its name is a dump of the compiled expression - and the shard
-- used to fail with `UNKNOWN_FUNCTION and(UInt8, less(UInt64, 1000 : UInt16))`. The body is left
-- uncompiled in a plan that is shipped, and the shard compiles it when it rebuilds the lambda.

DROP TABLE IF EXISTS t_05199;
DROP TABLE IF EXISTS t_05199_dist;

CREATE TABLE t_05199 (id UInt64, col Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05199 SELECT number, map('key' || toString(number % 3), number * 100) FROM numbers(10);
CREATE TABLE t_05199_dist AS t_05199 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05199);

SET prefer_localhost_replica = 0, compile_expressions = 1, min_count_to_compile_expression = 0;

SET serialize_query_plan = 1;
SELECT 'the plan is shipped';
SELECT 'mapExists', sum(mapExists((k, v) -> k LIKE '%2' AND v < 1000, col)) FROM t_05199_dist;
SELECT 'two comparisons', sum(mapExists((k, v) -> v > 100 AND v < 1000, col)) FROM t_05199_dist;
SELECT 'three comparisons', sum(mapExists((k, v) -> v > 0 AND v < 1000 AND v != 500, col)) FROM t_05199_dist;
SELECT 'a captured column', sum(mapExists((k, v) -> (v > id) AND (v < id + 1000), col)) FROM t_05199_dist;
SELECT 'arrayExists', sum(arrayExists(v -> v > 100 AND v < 1000, mapValues(col))) FROM t_05199_dist;
SELECT 'arrayMap', sum(arraySum(arrayMap(v -> (v > 100 AND v < 1000) ? 1 : 0, mapValues(col)))) FROM t_05199_dist;
SELECT 'a constant that fits UInt8', sum(mapExists((k, v) -> v > 0 AND v < 200, col)) FROM t_05199_dist;
SELECT 'OR instead of AND', sum(mapExists((k, v) -> k LIKE '%2' OR v < 1000, col)) FROM t_05199_dist;

SET serialize_query_plan = 0;
SELECT 'the same, plan not shipped';
SELECT 'mapExists', sum(mapExists((k, v) -> k LIKE '%2' AND v < 1000, col)) FROM t_05199_dist;
SELECT 'two comparisons', sum(mapExists((k, v) -> v > 100 AND v < 1000, col)) FROM t_05199_dist;
SELECT 'three comparisons', sum(mapExists((k, v) -> v > 0 AND v < 1000 AND v != 500, col)) FROM t_05199_dist;
SELECT 'a captured column', sum(mapExists((k, v) -> (v > id) AND (v < id + 1000), col)) FROM t_05199_dist;
SELECT 'arrayExists', sum(arrayExists(v -> v > 100 AND v < 1000, mapValues(col))) FROM t_05199_dist;
SELECT 'arrayMap', sum(arraySum(arrayMap(v -> (v > 100 AND v < 1000) ? 1 : 0, mapValues(col)))) FROM t_05199_dist;
SELECT 'a constant that fits UInt8', sum(mapExists((k, v) -> v > 0 AND v < 200, col)) FROM t_05199_dist;
SELECT 'OR instead of AND', sum(mapExists((k, v) -> k LIKE '%2' OR v < 1000, col)) FROM t_05199_dist;

DROP TABLE t_05199_dist;
DROP TABLE t_05199;
