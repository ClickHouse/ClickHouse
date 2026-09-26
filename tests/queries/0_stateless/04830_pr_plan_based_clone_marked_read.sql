-- Plan-based parallel replicas marks a coordinated read for serialization without attaching callbacks,
-- then clones the fragment again to build the initiator's local arm. The clone must reproduce that state
-- instead of resolving the callbacks from the context, which on an initiator holds none.
-- Reached whenever the marked read's context is not descended from the one holding the callbacks: a
-- system log storage builds a fresh context from the global context.
-- Related: #111677

DROP TABLE IF EXISTS t_clone_marked;

CREATE TABLE t_clone_marked (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_clone_marked SELECT number, number % 10 FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
-- The second clone sits inside canUseLocalPlanForParallelReplicas, which requires this setting.
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: CI's randomized automatic_parallel_replicas_mode can cost-decide against
-- parallel replicas, so the plan-based split would not engage.
SET automatic_parallel_replicas_mode = 0;

-- A system log is read through a fresh context built from the global context, which carries no
-- parallel-replicas callbacks. This is the shape that reached the logical error.
-- Without the flush an empty log has no parts, so the read is replaced with ReadFromPreparedSource
-- and never reaches the clone. `system.user_query_log` reads the backing `system.query_log` table,
-- which is what has to be flushed.
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 FROM system.user_query_log SETTINGS log_comment = '04830_clone_marked_user_query_log';

-- Parallel replicas really engaged for the affected shape, so `count() > 0` above is not being satisfied by
-- a plain local read: a regression that made this shape decline parallel replicas would leave it green.
-- The counter is incremented when the reading coordinator is destroyed, so it witnesses that a coordinator
-- was built for this query.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasQueryCount'] > 0 AS parallel_replicas_engaged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '04830_clone_marked_user_query_log'
SETTINGS enable_parallel_replicas = 0;

-- The initiator-local arm was built for the affected shape, which is the arm holding the clone under test.
-- The snapshot replica is pinned only on that arm, so a regression routing this shape to the remote-only
-- arm would stop exercising the clone while the counter above stayed green.
SYSTEM FLUSH LOGS query_log, text_log;
SELECT count() > 0 AS local_arm_built
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND logger_name = 'ParallelReplicasReadingCoordinator'
  AND message ILIKE '%is set as the snapshot replica%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
        AND current_database = currentDatabase() AND initial_query_id = query_id
        AND log_comment = '04830_clone_marked_user_query_log')
SETTINGS enable_parallel_replicas = 0, max_rows_to_read = 0; -- system.text_log can be really big

-- Results still match non-parallel execution, and count() is not multiplied across replicas.
SELECT count(), sum(b), min(a), max(a) FROM t_clone_marked WHERE a > 5;
SELECT b, count() FROM t_clone_marked GROUP BY b ORDER BY b;

-- Slow the initiator's local read so the remote replicas emit rows first: exercises the cloned local
-- arm after it is rewired with the coordinator's callbacks.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;
SELECT count(), sum(b) FROM t_clone_marked WHERE a > 5;
SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- The read is still distributed after the fix: a results-only test would also pass if the fix silently
-- stopped distributing.
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS has_local_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT sum(b) FROM t_clone_marked WHERE a > 5);

-- Positive control for the context fallback: an ordinary (not plan-based) parallel-replicas read still
-- resolves its callbacks from the context on a follower. The initiator has no local arm here, so the
-- result can only be produced by followers that resolved the callbacks.
SELECT count(), sum(b) FROM t_clone_marked WHERE a > 5
SETTINGS parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 0;

-- Every arm of a coordinated read has to succeed, including the ones a replica rebuilds from the
-- serialized plan. Those run under their own query id and the initiator still returns the right rows
-- without them, so the result comparisons above cannot see them fail. A callback missing at plan
-- construction time is logged before the query starts, so both exception types count.
SYSTEM FLUSH LOGS query_log;
SELECT count() AS failed_queries
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_clone_marked;
