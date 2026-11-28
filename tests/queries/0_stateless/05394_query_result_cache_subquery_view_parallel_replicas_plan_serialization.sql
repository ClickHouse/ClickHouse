-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- A view whose body has an `IN` subquery, read with parallel replicas and `serialize_query_plan = 1`.
-- `createRemotePlanForParallelReplicas` builds the logical plan that is shipped to the replicas with
-- `build_logical_plan`. The view body must not get the non-serializable query result cache steps in
-- that plan, otherwise the query fails with `Method serialize is not implemented for
-- StreamInQueryResultCache`. Repeat the queries, so the second run also covers the cache read path.

SYSTEM DROP QUERY CACHE;

DROP VIEW IF EXISTS v_qrc_view_pr;
DROP TABLE IF EXISTS t_qrc_view_pr;
CREATE TABLE t_qrc_view_pr (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_view_pr SELECT number FROM numbers(1000);
CREATE VIEW v_qrc_view_pr AS SELECT k FROM t_qrc_view_pr WHERE k IN (SELECT k FROM t_qrc_view_pr WHERE k < 10);

-- Subquery caching is a Planner feature.
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

-- 0 + 1 + ... + 9 = 45.
SELECT sum(k) FROM v_qrc_view_pr;
SELECT sum(k) FROM v_qrc_view_pr;
SELECT sum(k) FROM t_qrc_view_pr WHERE k IN (SELECT k FROM v_qrc_view_pr);
SELECT sum(k) FROM t_qrc_view_pr WHERE k IN (SELECT k FROM v_qrc_view_pr);

DROP VIEW v_qrc_view_pr;
DROP TABLE t_qrc_view_pr;
SYSTEM DROP QUERY CACHE;
