-- Tags: no-sanitizers
-- no-sanitizers: needs enough data and small blocks to collect statistics and make parallel replicas
--                cost-beneficial, which is too slow under sanitizers (as for 05024).

-- Automatic parallel replicas must work the same no matter how the query receives its settings. A
-- query carrying `automatic_parallel_replicas_mode` in its own `SETTINGS` clause used to silently run
-- without parallel replicas: the plan builder disables the heuristic on the context it hands to the
-- nested interpreter (that plan is meant to be one with enforced parallel replicas), but the nested
-- interpreter re-applied the clause on top of it, so the nested plan came back with no read from the
-- other replicas and the optimization gave up. Settings written after `FORMAT` are not re-applied,
-- which is why the same query worked when its `SETTINGS` clause was written there.

DROP TABLE IF EXISTS t;

CREATE TABLE t(WatchID UInt64, ClientIP UInt32, ResolutionWidth UInt16) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=128;

SET enable_parallel_replicas=1, parallel_replicas_local_plan=1,
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

-- `automatic_parallel_replicas_mode` is passed in the query's own `SETTINGS` clause, which is the whole
-- point of the test: everything else is passed out of band, exactly as in 05024.

-- Query 0: empty cache, collect statistics, no parallel replicas yet
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
    SETTINGS automatic_parallel_replicas_mode=1, log_comment='05059_autopr_select_settings_query_0' FORMAT Null;

-- Query 1: the same replica sub-plan, statistics are already collected, parallel replicas are enabled
SELECT WatchID, ClientIP, COUNT(*) AS c, AVG(ResolutionWidth) FROM t GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10
    SETTINGS automatic_parallel_replicas_mode=1, log_comment='05059_autopr_select_settings_query_1' FORMAT Null;

SET enable_parallel_replicas=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05059_autopr_select_settings_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t;
