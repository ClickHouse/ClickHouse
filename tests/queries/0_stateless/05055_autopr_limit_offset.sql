-- Verify how query plans containing `Limit` and `Offset` steps interact with the automatic parallel
-- replicas optimization.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=0;
-- Keep `effective_max_reading_threads` from capping, so the comparison is decided by the network term.
SET merge_tree_min_bytes_per_task_for_remote_reading=65536;

-- `value` is incompressible so the input and output byte estimators agree; the check rests on their ratio.
INSERT INTO t SELECT number, rand64() FROM numbers(1e6) SETTINGS max_insert_threads = 1;
-- Single part, so the input estimate is stable: every part rounds its tail read up to a whole granule,
-- and with several of them the estimate grows enough to flip the comparison.
OPTIMIZE TABLE t FINAL;

-- A bare `OFFSET`: the boundary is the `Sorting` below the `Union`, the `Offset` runs on the initiator.
SELECT value FROM t ORDER BY value OFFSET 100 FORMAT Null SETTINGS log_comment='05055_autopr_offset_query';

-- The boundary is the shard `Limit`. Query 0 collects the statistics, query 1 decides with them.
-- The limit puts O at ~0.3 of I, inside the band (I/6, I/2) where the divisor decides the outcome:
-- local I/4 beats replicas I/12 + O, but would lose to I/12 + O/3 if the output were treated as
-- partitioned. O has to move by 1.7x either way before the decision flips.
SELECT value FROM t ORDER BY value LIMIT 300000 FORMAT Null SETTINGS log_comment='05055_autopr_limit_query_0';
SELECT value FROM t ORDER BY value LIMIT 300000 FORMAT Null SETTINGS log_comment='05055_autopr_limit_query_1';

-- The negative and fractional variants are separate steps, each of which can reject the whole plan.
-- `LIMIT -3` is the one that can be the boundary itself. Mode 2 forces recollection: otherwise the two
-- `Sorting`-boundary shapes would reuse the entry the bare-OFFSET query cached under the same hash.
SELECT value FROM t ORDER BY value LIMIT -3 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_negative_limit';
SELECT value FROM t ORDER BY value OFFSET -5 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_negative_offset';
SELECT value FROM t ORDER BY value LIMIT 0.3 OFFSET 0.2 FORMAT Null SETTINGS automatic_parallel_replicas_mode=2, log_comment='05055_autopr_fractional';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The bare-`OFFSET` plan passes the gate, so both input and output bytes are recorded.
SELECT
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS input_stats_collected,
    ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment = '05055_autopr_offset_query') AND (type = 'QueryFinish')
FORMAT TSVWithNames;

-- Query 1 reuses query 0's statistics and must decide against parallel replicas, because every replica
-- would ship its own `LIMIT 300000` worth of rows.
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05055_autopr_limit_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

-- All three sibling shapes pass the gate and collect statistics.
SELECT log_comment,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS input_stats_collected,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS output_stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment IN ('05055_autopr_negative_limit', '05055_autopr_negative_offset', '05055_autopr_fractional')) AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
