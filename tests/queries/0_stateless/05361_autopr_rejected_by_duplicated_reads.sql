-- A candidate declined by `automatic_parallel_replicas_max_duplicated_read_ratio` after winning the cost
-- model's time comparison ends in `AutoParallelReplicasRejectedByDuplicatedReads`, so that every query
-- reaching the cost model ends in an event saying why. The setup is the one of
-- `05293_autopr_duplicated_read_ratio_gate`, which checks the decision itself.

DROP TABLE IF EXISTS probe_side_05361;
DROP TABLE IF EXISTS build_side_05361;

CREATE TABLE probe_side_05361 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE build_side_05361 (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO probe_side_05361 SELECT number FROM numbers(200000);
INSERT INTO build_side_05361 SELECT number FROM numbers(600000);

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
-- Keep the build side on the right and unfiltered: the join order optimizer would otherwise swap the
-- sides, and a runtime filter would prune the probe side - either changes which read is coordinated.
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_randomize = 0;

-- The decision is made from statistics of an earlier execution, so this one only collects them.
SELECT sum(p.id + b.id) FROM probe_side_05361 AS p INNER JOIN build_side_05361 AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'dup_reads_1_collect';

-- Declined: the build side is read by every replica and is three times the coordinated read. Declined at
-- the boundary the first run remembered, without building the plan.
SELECT sum(p.id + b.id) FROM probe_side_05361 AS p INNER JOIN build_side_05361 AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'dup_reads_2_at_default';

-- Taken with the gate disabled, which is what makes the row above meaningful: the candidate is
-- otherwise worth taking, so the default ratio is the only reason it was declined. Taking it needs the
-- plan, so the decision at the remembered boundary is made again with it, and counted once.
SET automatic_parallel_replicas_max_duplicated_read_ratio = 1;
SELECT sum(p.id + b.id) FROM probe_side_05361 AS p INNER JOIN build_side_05361 AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'dup_reads_3_disabled';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS collected,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS evaluated,
       ProfileEvents['AutoParallelReplicasRejectedByDuplicatedReads'] AS rejected_by_duplicated_reads,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied,
       ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] AS decided_without_plan
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND log_comment LIKE 'dup_reads_%' AND type = 'QueryFinish'
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE probe_side_05361;
DROP TABLE build_side_05361;
