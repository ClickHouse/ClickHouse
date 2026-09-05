-- Automatic parallel replicas do not build the parallel-replicas plan at all when index analysis
-- already shows that the query reads less than `automatic_parallel_replicas_min_bytes_per_replica`
-- per replica. The observable consequence is that no dataflow statistics are collected for such a
-- query, not even on the very first run when the statistics cache is still empty for it.

DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_large;

CREATE TABLE t_small(key UInt64, value String) ENGINE = MergeTree ORDER BY key;
-- `key` is the only column the queries below read, and the gate sizes a read by its compressed
-- bytes. Storing it uncompressed keeps that size a property of the test rather than of how well
-- sequential integers happen to compress, which varies with the server's compression settings.
CREATE TABLE t_large(key UInt64 CODEC(NONE), value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t_small SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_large SELECT number, toString(number) FROM numbers(3e6);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=1048576;

-- The whole table is far below the threshold, so the optimization gives up before building the
-- parallel-replicas plan and no statistics are collected.
SELECT count() FROM t_small GROUP BY key % 10 FORMAT Null SETTINGS log_comment='05045_gate_small';

-- Enough data to be worth considering (24 MB of `key`, ~8 MB per replica against the 1 MiB
-- threshold), so the plan is built and statistics are collected as usual.
SELECT count() FROM t_large GROUP BY key % 10 FORMAT Null SETTINGS log_comment='05045_gate_large';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 AS pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05045_gate_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_small;
DROP TABLE t_large;
