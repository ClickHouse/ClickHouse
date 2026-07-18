-- Regression test: a stateful function (e.g. `logTrace`, `neighbor`) in the SELECT list of an
-- otherwise trivial `LIMIT` query requires a single deterministic input stream. Parallel replicas
-- split the read across replicas and interleave the rows the stateful expression observes, so they
-- must be disabled for such a query.
--
-- The trivial-`LIMIT` guard used to force only a single *local* stream, while the parallel-replicas
-- rewrite still fired (it keys off `trivial_limit`, which the guard leaves at 0). This is fixed for
-- both the planner (`PlannerJoinTree`) and the old-analyzer interpreter
-- (`InterpreterSelectQuery::adjustParallelReplicasAfterAnalysis`).

DROP TABLE IF EXISTS t_pr_stateful;

CREATE TABLE t_pr_stateful (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_pr_stateful SELECT number, number * 10 FROM numbers(1000);

SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Sanity: a plain query over the same table DOES use parallel replicas in this setup.
SELECT count(), sum(k) FROM t_pr_stateful SETTINGS log_comment = '04551_plain', enable_analyzer = 1 FORMAT Null;

-- A stateful function in the select list of a trivial-`LIMIT` query must NOT use parallel replicas.
SELECT ignore(logTrace('04551')), k FROM t_pr_stateful LIMIT 1 SETTINGS log_comment = '04551_stateful_new', enable_analyzer = 1 FORMAT Null;
SELECT ignore(logTrace('04551')), k FROM t_pr_stateful LIMIT 1 SETTINGS log_comment = '04551_stateful_old', enable_analyzer = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'plain', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04551_plain' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

SELECT 'stateful_new', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04551_stateful_new' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

SELECT 'stateful_old', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04551_stateful_old' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_pr_stateful;
