-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- With plan-based parallel replicas the plan is built locally and `applyParallelReplicas` later
-- serializes a fragment of it - including the plans of `IN` subquery sets - for the replicas. The
-- query result cache steps of a subquery are not serializable, so a fragment holding one is kept
-- local: the cache would silently disable parallel replicas, and on a warm cache the remote/local
-- decision would depend on cache contents. The Planner-level subquery cache is therefore not used
-- under plan-based parallel replicas - neither via `query_cache_for_subqueries` nor via an explicit
-- `SETTINGS use_query_cache = 1` of the subquery.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_pr_pb;
CREATE TABLE t_qrc_pr_pb (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_pr_pb SELECT number FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;

-- 0 + 1 + ... + 9 = 45.
SELECT sum(k) FROM t_qrc_pr_pb WHERE k IN (SELECT k FROM t_qrc_pr_pb WHERE k < 10)
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1;

SELECT sum(k) FROM t_qrc_pr_pb WHERE k IN (SELECT k FROM t_qrc_pr_pb WHERE k < 20 SETTINGS use_query_cache = 1);

-- A subquery cannot escape the ban by turning plan-based parallel replicas off in its own SETTINGS.
SELECT sum(k) FROM t_qrc_pr_pb WHERE k IN (SELECT k FROM t_qrc_pr_pb WHERE k < 30 SETTINGS use_query_cache = 1, parallel_replicas_plan_based = 0);

SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_pr_pb%';

-- Without plan-based parallel replicas the subquery cache keeps working.
SET parallel_replicas_plan_based = 0, enable_parallel_replicas = 0;
SELECT sum(k) FROM t_qrc_pr_pb WHERE k IN (SELECT k FROM t_qrc_pr_pb WHERE k < 10 SETTINGS use_query_cache = 1);
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_pr_pb%';

DROP TABLE t_qrc_pr_pb;
SYSTEM DROP QUERY CACHE;
