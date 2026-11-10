-- Tags: no-fasttest
-- Plan-based parallel replicas build an ordinary local plan and then serialize a fragment of it for the
-- replicas. A lambda body becomes `ExpressionActions` while that plan is built, so a JIT-compiled lambda
-- body would be shipped as a node whose name is a dump of the compiled expression, and the replica would
-- fail with `UNKNOWN_FUNCTION`. The body is left uncompiled in such a plan.
-- Without a local plan every replica, the initiator's own one included, reads through the shipped
-- fragment, so a replica failure is not hidden by the initiator reading the rest itself.

DROP TABLE IF EXISTS t_05321;

CREATE TABLE t_05321 (id UInt64, col Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05321 SELECT number, map('key' || toString(number % 3), number * 100) FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 0;
SET automatic_parallel_replicas_mode = 0;
SET compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT 'mapExists', count() FROM t_05321 WHERE mapExists((k, v) -> k LIKE '%2' AND v < 100000, col);
SELECT 'arrayExists', count() FROM t_05321 WHERE arrayExists(v -> v > 100 AND v < 50000, mapValues(col));
SELECT 'a captured column', sum(mapExists((k, v) -> (v > id) AND (v < id + 1000), col)) FROM t_05321;
SELECT 'arrayMap', sum(arraySum(arrayMap(v -> (v > 100 AND v < 50000) ? 1 : 0, mapValues(col)))) FROM t_05321;

DROP TABLE t_05321;
