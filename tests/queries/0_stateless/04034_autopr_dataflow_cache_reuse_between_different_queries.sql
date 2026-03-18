-- Tags: no-sanitizers
-- no-sanitizers: too slow

-- Verify that statistics are reused between queries with identical "distributed to replicas" sub-plans.

DROP TABLE IF EXISTS t;

CREATE TABLE t(WatchID UInt64, ClientIP UInt32, ResolutionWidth UInt16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=128;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- May disable the usage of parallel replicas
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t SELECT number % 1000, number % 500, number % 200 FROM numbers(5e5);

-- Query 0: GROUP BY without HAVING — empty cache, collect stats
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='04034_autopr_dataflow_cache_reuse_between_different_queries_query_0';

-- Query 1: GROUP BY with HAVING — should reuse stats from query 0 (same replica sub-plan) and enable parallel replicas
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP HAVING c > 1 ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='04034_autopr_dataflow_cache_reuse_between_different_queries_query_1';

-- Query 2: Repeat query without HAVING — stats already available, should also enable parallel replicas
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='04034_autopr_dataflow_cache_reuse_between_different_queries_query_2';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '04034_autopr_dataflow_cache_reuse_between_different_queries_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
