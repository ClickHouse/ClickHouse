-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- `FunctionCapture::isDeterministic` must report the determinism of the lambda body: the outer
-- `ActionsDAG` only sees the capture node, so without it a filter calling a non-deterministic
-- function inside a lambda would look deterministic and its granule statistics would be stored
-- in the query condition cache and reused by later queries.

DROP TABLE IF EXISTS t_qcc_lambda;

CREATE TABLE t_qcc_lambda (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;

-- the query condition cache stores nothing for small tables
INSERT INTO t_qcc_lambda SELECT number, number FROM numbers(1000000);

-- the old analyzer never stores such filters in the query condition cache, pin the new one
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;

SELECT '= a filter with a non-deterministic lambda body is not cached =';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lambda WHERE arrayExists(x -> x = a AND x = 4242 AND rand(x) > 0, [b]) FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '= the same filter with a deterministic lambda body is cached =';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_lambda WHERE arrayExists(x -> x = a AND x = 4242, [b]) FORMAT Null;
SELECT count() > 0 FROM system.query_condition_cache;

DROP TABLE t_qcc_lambda;
