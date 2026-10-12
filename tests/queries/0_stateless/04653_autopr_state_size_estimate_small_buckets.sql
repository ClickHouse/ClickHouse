-- Tags: no-sanitizers
-- no-sanitizers: too slow

-- Automatic parallel replicas estimates how many bytes the aggregate states would take on the wire by
-- serializing a sample of the states of every two-level bucket through a compressing buffer. With an
-- early conversion to a two-level hash table every bucket holds only a handful of states, so the sample
-- is smaller than the per-block framing of the compressed format. The estimate must not conclude from
-- that framing that the states grow under compression, otherwise it inflates the output-bytes estimate
-- and parallel replicas get rejected for a query that does benefit from them.

DROP TABLE IF EXISTS t;

CREATE TABLE t(WatchID UInt64, ClientIP UInt32, ResolutionWidth UInt16) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- Convert to a two-level hash table from the very first block, so that every bucket holds only a few states.
SET group_by_two_level_threshold=1, group_by_two_level_threshold_bytes=1;

-- May disable the usage of parallel replicas
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t SELECT number % 1000, number % 500, number % 200 FROM numbers(5e5);

-- The first query has an empty cache and only collects the statistics.
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='04653_autopr_state_size_estimate_small_buckets_query_0';

-- The second query reuses them and has to enable parallel replicas.
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='04653_autopr_state_size_estimate_small_buckets_query_1';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '04653_autopr_state_size_estimate_small_buckets_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
