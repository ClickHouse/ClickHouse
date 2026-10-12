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

-- The columns are the read step's own list, which is what this node was asked to read.
-- Pinned to the latest probe within a short window, so a run in a reused database never reads the
-- worker rows of an earlier one.
SELECT 'every worker of the probe names the table, the database and the column it read';
WITH (
    SELECT argMax(query_id, event_time_microseconds)
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query = 1
      AND event_date >= yesterday() AND event_time >= now() - INTERVAL 30 MINUTE
      AND current_database = currentDatabase() AND log_comment = 'pr_worker_access_probe'
) AS probe_query_id
SELECT count() > 0
   AND countIf(NOT has(tables, currentDatabase() || '.t_pr_worker_access')) = 0
   AND countIf(NOT has(databases, currentDatabase())) = 0
   AND countIf(NOT has(columns, currentDatabase() || '.t_pr_worker_access.a')) = 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 30 MINUTE
  AND initial_query_id = probe_query_id;

DROP TABLE t_pr_worker_access SYNC;
