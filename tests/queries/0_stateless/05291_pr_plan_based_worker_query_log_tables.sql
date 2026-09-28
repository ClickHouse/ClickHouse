-- A read that arrives at a replica as a shipped plan is executed without the planner, which is what
-- records the query access info that fills `databases`, `tables` and `columns` in `system.query_log`.
-- Without that, a worker's row names nothing it read, and its work cannot be attributed to a table -
-- by an audit, by a quota, or by a test selecting worker queries by database.

DROP TABLE IF EXISTS t_pr_worker_access SYNC;

CREATE TABLE t_pr_worker_access (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_worker_access SELECT number FROM numbers(1000);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;
-- Every read happens on a worker, so the rows checked below are all shipped-plan reads.
SET parallel_replicas_local_plan = 0;

SELECT sum(a) FROM t_pr_worker_access FORMAT Null SETTINGS log_comment = 'pr_worker_access_probe';

SYSTEM FLUSH LOGS query_log;

SELECT 'every worker names the table it read';
SELECT count() > 0 AND countIf(NOT has(tables, currentDatabase() || '.t_pr_worker_access')) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query = 1 AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = 'pr_worker_access_probe');

-- Columns are not asserted: the step carries the storage read list after plan optimizations, not the
-- planner's column set, so it is deliberately not reported.
SELECT 'and the database';
SELECT countIf(NOT has(databases, currentDatabase())) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
  AND initial_query_id IN (
      SELECT query_id FROM system.query_log
      WHERE type = 'QueryFinish' AND is_initial_query = 1 AND event_date >= yesterday()
        AND current_database = currentDatabase() AND log_comment = 'pr_worker_access_probe');

DROP TABLE t_pr_worker_access SYNC;
