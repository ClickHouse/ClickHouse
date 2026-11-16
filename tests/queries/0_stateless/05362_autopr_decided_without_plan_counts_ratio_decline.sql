-- Checks that `AutomaticParallelReplicasDecidedWithoutBuildingPlan` counts every decline taken at a remembered
-- boundary, not only the one from the time comparison. Here the time comparison favours parallel replicas and
-- `automatic_parallel_replicas_max_duplicated_read_ratio` declines the query instead, as in `05293`.

DROP TABLE IF EXISTS probe_dwp;
DROP TABLE IF EXISTS build_dwp;

CREATE TABLE probe_dwp (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE build_dwp (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO probe_dwp SELECT number FROM numbers(2000000);
INSERT INTO build_dwp SELECT number FROM numbers(6000000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1;
-- The `distributed plan` checks turn `serialize_query_plan` on, which ships a plan fragment instead
-- and takes a different path to the same decision; pin it so the assertion below stays about the ratio.
SET serialize_query_plan = 0;
-- A small task size leaves the coordinated read splittable enough that distributing wins the time
-- comparison, so that the ratio alone decides the outcome.
SET merge_tree_min_bytes_per_task_for_remote_reading = 4096;
-- The estimated read depends on the compression of the parts, which randomized settings change, and
-- can fall below the per-replica floor; that gate would then decline the candidate on its own.
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- Distributed, every replica reads the whole build side, which takes the query to about the test
-- profile's `max_rows_to_read`.
SET max_rows_to_read = 0;
-- Keep the build side on the right and unfiltered: the join order optimizer would otherwise swap the
-- sides, and a runtime filter would prune the probe side - either changes which read is coordinated.
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_randomize = 0;

-- Collects the statistics and remembers the boundary, building the plan with parallel replicas to find it.
SELECT sum(p.id + b.id) FROM probe_dwp AS p INNER JOIN build_dwp AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'dwp_1_collect';
-- Declined by the ratio at the remembered boundary, so the plan is not built.
SELECT sum(p.id + b.id) FROM probe_dwp AS p INNER JOIN build_dwp AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'dwp_2_declined';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS built_the_plan,
    ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] > 0 AS decided_without_building_the_plan,
    ProfileEvents['ParallelReplicasUsedCount'] > 0 AS used_parallel_replicas
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `dwp_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'dwp_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE probe_dwp;
DROP TABLE build_dwp;
