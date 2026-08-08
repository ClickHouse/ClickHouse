-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- Regression test for `Method serialize is not implemented for StreamInQueryResultCache`.
--
-- With `query_cache_for_subqueries = 1` the Planner wraps an `IN` subquery plan with
-- `StreamInQueryResultCacheStep` (write path) or replaces it with `ReadFromQueryResultCacheStep`
-- (read path). Those steps hold node-local state - a `QueryResultCacheWriter`, or the cached chunks -
-- which has no serialized representation. A *logical* plan is not executed where it is built: parallel
-- replicas with `serialize_query_plan = 1` build one in `createRemotePlanForParallelReplicas` and ship
-- it to the replicas, so planting the cache steps there failed the whole query in
-- `QueryPlan::ensureSerialized`. The cache is populated by the plan the initiator executes itself, so
-- the query result cache must still work for the subquery.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_pr;
CREATE TABLE t_qrc_pr (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_pr SELECT number FROM numbers(1000);

-- Subquery caching is a Planner feature: with the old analyzer no subquery cache entry is created at
-- all, so the last check below would trivially return 0. Pin the analyzer, because some CI jobs run
-- the whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;

SET use_query_cache = 1;
SET query_cache_for_subqueries = 1;
SET serialize_query_plan = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_local_plan = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

-- `sum` rather than `count`, so the query is not answered by the trivial count optimization (which
-- disables parallel replicas). 0 + 1 + ... + 9 = 45.
SELECT sum(k) FROM t_qrc_pr WHERE k IN (SELECT k FROM t_qrc_pr WHERE k < 10);

-- The subquery result is cached by the plan the initiator runs, not by the plan shipped to replicas.
SET use_query_cache = 0;
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_pr WHERE k < 10%';

DROP TABLE t_qrc_pr;
SYSTEM DROP QUERY CACHE;
