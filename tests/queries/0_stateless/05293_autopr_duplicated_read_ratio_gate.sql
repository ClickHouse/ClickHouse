-- Checks `automatic_parallel_replicas_max_duplicated_read_ratio`: a candidate that wins the cost
-- model's time comparison is still declined when too much of the reading is repeated by every
-- replica. Only the coordinated read is split; the build side below is read in full on every
-- replica, so distributing this query would multiply that work without making the query faster.

DROP TABLE IF EXISTS probe_side;
DROP TABLE IF EXISTS build_side;

CREATE TABLE probe_side (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE build_side (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO probe_side SELECT number FROM numbers(2000000);
INSERT INTO build_side SELECT number FROM numbers(6000000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, max_parallel_replicas = 3,
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

-- The decision is made from statistics of an earlier execution, so this one only collects them.
SELECT sum(p.id + b.id) FROM probe_side AS p INNER JOIN build_side AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'ratio_gate_1_collect';

-- Declined: the build side is read by every replica and is three times the coordinated read.
SELECT sum(p.id + b.id) FROM probe_side AS p INNER JOIN build_side AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'ratio_gate_2_at_default';

-- Taken with the gate disabled, which is what makes the row above meaningful: the candidate is
-- otherwise worth taking, so the default ratio is the only reason it was declined.
SET automatic_parallel_replicas_max_duplicated_read_ratio = 1;
SELECT sum(p.id + b.id) FROM probe_side AS p INNER JOIN build_side AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'ratio_gate_3_disabled';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS parallel_replicas_used
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND log_comment LIKE 'ratio_gate_%' AND type = 'QueryFinish'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE probe_side;
DROP TABLE build_side;
