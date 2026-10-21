-- An `ORDER BY` in a subquery whose order the outer query does not use is removed by
-- `query_plan_remove_redundant_sorting`, so it must not keep the outer query from going to the replicas:
-- they used to sort the subquery for the initiator to aggregate, and the initiator dropped the order.

DROP TABLE IF EXISTS t_pr_order;
CREATE TABLE t_pr_order (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_pr_order SELECT number, number * 3 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- The plan-based implementation decides on the optimized plan, where the sorting is already removed, and shows other steps in `EXPLAIN`.
SET parallel_replicas_plan_based = 0;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET query_plan_enable_optimizations = 1;
SET query_plan_remove_redundant_sorting = 1;

SELECT 'sum over a sorted subquery: the replicas aggregate, nobody sorts';
SELECT
    countIf(explain LIKE '%Sorting%') AS sortings,
    countIf(explain LIKE '%ReadFromRemoteParallelReplicas%Query: SELECT sum(%') AS replicas_aggregate
FROM (EXPLAIN SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key));

SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key);
SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key)
SETTINGS enable_parallel_replicas = 0;

SELECT 'automatic parallel replicas match the plan and collect statistics for it';
-- Matching failed on the sorting the replicas plan kept (`Cannot find step with matching hash`),
-- and then no statistics were collected.
SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key)
FORMAT Null
SETTINGS automatic_parallel_replicas_mode = 2, log_comment = '05394_automatic';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0,
    ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND log_comment = '05394_automatic' AND type = 'QueryFinish';

SELECT 'the sorting is not removed without query_plan_remove_redundant_sorting';
SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%Query: SELECT sum(%') AS replicas_aggregate
FROM (EXPLAIN SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key))
SETTINGS query_plan_remove_redundant_sorting = 0;

SELECT 'LIMIT reads the sorted rows: the subquery is not redundant';
SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%Query: SELECT sum(%') AS replicas_aggregate
FROM (EXPLAIN SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key LIMIT 1000));

SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key LIMIT 1000);
SELECT sum(cityHash64(key, value)) FROM (SELECT key, value FROM t_pr_order WHERE value % 7 = 1 ORDER BY key LIMIT 1000)
SETTINGS enable_parallel_replicas = 0;

SELECT 'groupArray depends on the order: the subquery is not redundant';
SELECT countIf(explain LIKE '%ReadFromRemoteParallelReplicas%Query: SELECT groupArray(%') AS replicas_aggregate
FROM (EXPLAIN SELECT groupArray(key) FROM (SELECT key FROM t_pr_order WHERE value % 7 = 1 ORDER BY key DESC));

SELECT cityHash64(groupArray(key)) FROM (SELECT key FROM t_pr_order WHERE value % 7 = 1 ORDER BY key DESC);
SELECT cityHash64(groupArray(key)) FROM (SELECT key FROM t_pr_order WHERE value % 7 = 1 ORDER BY key DESC)
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_pr_order;
