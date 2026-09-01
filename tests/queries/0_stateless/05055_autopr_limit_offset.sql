-- Verify how query plans containing `Limit` and `Offset` steps interact with the automatic parallel
-- replicas optimization.
--
-- 1. `OffsetStep` did not support dataflow statistics collection, and since
--    `considerEnablingParallelReplicas` uses that predicate as a whole-plan gate, any plan carrying a
--    bare `OffsetStep` was rejected outright (`optimizeTree: Some steps in the plan don't support
--    dataflow statistics collection ... Unsupported steps: Offset_...`) and no statistics were
--    gathered. `LIMIT n OFFSET m` is unaffected - the planner folds it into a single `LimitStep` - so
--    only an `OFFSET` without a `LIMIT` produces such a plan.
--
-- 2. The cost model divides the boundary's `output_bytes` by the number of replicas, which assumes the
--    replicas partition that output between them. A row-limiting boundary is replicated instead: a
--    shard `LIMIT n` makes every replica emit up to `n` rows, so each of them ships the full
--    `output_bytes`. Dividing anyway underestimated the parallel-replicas plan by `max_parallel_replicas`.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=0;
-- Keep `effective_max_reading_threads` from becoming the binding cap, so that the comparison below is
-- decided by the network term rather than by the reading term.
SET merge_tree_min_bytes_per_task_for_remote_reading=65536;

-- `value` is incompressible, which makes the two byte estimators agree: the input side scales the read
-- bytes by the part's compressed/uncompressed ratio, the output side by a sampled serialization, and
-- both land near the raw size. The ratio between input and output bytes is what the check below rests on.
INSERT INTO t SELECT number, rand64() FROM numbers(1e6) SETTINGS max_insert_threads = 1;
-- Collapse to a single part. The comparison below rests on the ratio between the estimated input and
-- output bytes, so the input estimate has to be stable. Every part rounds the read of its tail up to a
-- whole granule, so with several parts (or a merge landing mid-test) the read reports more rows than
-- the table holds and the estimated input bytes grow by up to ~1.9x - enough to flip the comparison.
OPTIMIZE TABLE t FINAL;

-- A bare `OFFSET` (no `LIMIT`). The replica-output boundary is the `Sorting` step below the `Union`;
-- the `Offset` step itself is computed on the initiator, because OFFSET means skipping rows of the
-- entire query result rather than of each shard. Statistics must be collected: before `OffsetStep`
-- reported support, the plan was rejected by the "simple enough" gate and nothing was recorded.
SELECT value FROM t ORDER BY value OFFSET 100 FORMAT Null SETTINGS log_comment='05055_autopr_offset_query';

-- `ORDER BY ... LIMIT n`: the replica-output boundary is the shard `Limit` above the `Sorting`, which
-- records the post-limit output. Query 0 collects the statistics (the cache is empty), query 1 makes
-- the decision with them.
--
-- The limit is chosen so that output_bytes is ~0.3 of input_bytes, which is the band where the divisor
-- decides. With max_threads=4 and max_parallel_replicas=3:
--   local    = I/4                = 0.25 I
--   replicas = I/12 + O/1         = 0.38 I   -> local is cheaper, parallel replicas NOT enabled
-- Treating the limited output as partitioned (the previous `O/3`) gives 0.18 I and enables parallel
-- replicas for a query that does not benefit from them. The decision flips only once O leaves the band
-- (I/6, I/2), i.e. not before O moves by 1.7x in either direction, so the check is not knife-edge.
SELECT value FROM t ORDER BY value LIMIT 300000 FORMAT Null SETTINGS log_comment='05055_autopr_limit_query_0';
SELECT value FROM t ORDER BY value LIMIT 300000 FORMAT Null SETTINGS log_comment='05055_autopr_limit_query_1';

-- The negative and fractional variants of LIMIT/OFFSET are separate steps, and the gate rejects a plan
-- for any one of them that does not report support. `LIMIT -n` returns the last `n` rows, and unlike the
-- offset variants it can be pushed to the shard (`addPreliminaryLimitStep` marks it as a shard limit), so
-- it can be the replica-output boundary itself - which is why it carries a collector and is priced as
-- replicated, exactly like `LIMIT n`.
-- `automatic_parallel_replicas_mode=2` forces the statistics to be recollected on every run. Without it
-- the two shapes whose boundary is the `Sorting` step would reuse the entry the bare-OFFSET query above
-- already cached under the same plan hash, and would record nothing - which says nothing about the gate.
SELECT value FROM t ORDER BY value LIMIT -3 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_negative_limit';
SELECT value FROM t ORDER BY value OFFSET -5 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_negative_offset';
SELECT value FROM t ORDER BY value LIMIT 0.3 OFFSET 0.2 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_fractional';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The plan with a bare `OFFSET` passes the gate now, so statistics are collected at the `Sorting`
-- boundary: both input and output bytes are recorded.
SELECT
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS input_stats_collected,
    ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '05055_autopr_offset_query') AND (type = 'QueryFinish')
FORMAT TSVWithNames;

-- Query 0 collects the statistics for the `Limit` boundary; query 1 reuses them and must decide against
-- parallel replicas, because every replica would ship its own `LIMIT 300000` worth of rows.
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05055_autopr_limit_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

-- All three sibling shapes now pass the "simple enough" gate and collect statistics. Before they
-- reported support the plan was rejected outright with `Unsupported steps: NegativeLimit_...`,
-- `NegativeOffset_...` or `FractionalLimit_...` and nothing was recorded.
SELECT log_comment,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS input_stats_collected,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment IN ('05055_autopr_negative_limit', '05055_autopr_negative_offset', '05055_autopr_fractional')) AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
