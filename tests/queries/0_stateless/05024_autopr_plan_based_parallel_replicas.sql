-- Tags: no-sanitizers
-- no-sanitizers: needs enough data and small blocks to collect statistics and make parallel replicas
--                cost-beneficial, which is too slow under sanitizers (as for 03783 / 04034).

-- Automatic parallel replicas with the plan-based implementation (`parallel_replicas_plan_based`).
-- It builds the same plan shape as the query-based one - a `Union` of the local branch and a branch
-- reading from the other replicas - so the statistics collector must be installed for it too. The
-- first query finds an empty cache and collects statistics, the second one reuses them and enables
-- parallel replicas.

DROP TABLE IF EXISTS t;

CREATE TABLE t(WatchID UInt64, ClientIP UInt32, ResolutionWidth UInt16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=128;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET parallel_replicas_plan_based=1;

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- May disable the usage of parallel replicas
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- Merge the partial aggregation results of the replicas without `GroupingAggregatedTransform`: the
-- memory efficient merging rarely hits `Logical error: 'Bucket N is pushed twice'`
-- (https://github.com/ClickHouse/ClickHouse/issues/115663), which has nothing to do with what this
-- test checks - the decision of automatic parallel replicas, not how the partial results are merged.
SET distributed_aggregation_memory_efficient=0;

INSERT INTO t SELECT number % 1000, number % 500, number % 200 FROM numbers(5e5);

-- Query 0: empty cache, collect statistics, no parallel replicas yet
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='05024_autopr_plan_based_query_0';

-- Query 1: the same replica sub-plan, statistics are already collected, parallel replicas are enabled
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='05024_autopr_plan_based_query_1';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05024_autopr_plan_based_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
