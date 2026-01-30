-- Tags: no-sanitizers, long
-- no-sanitizers: too slow
-- long: for flaky check

DROP TABLE IF EXISTS t;

-- index_granularity: to be able to produce small blocks from reading
CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=128;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- For runs with the old analyzer
SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- May disable the usage of parallel replicas
SET automatic_parallel_replicas_min_bytes_per_replica=0;

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t SELECT toString(number), number FROM numbers(1e4);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_0'; -- empty cache, don't apply optimization, collect stats

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_1'; -- stats available, don't apply since no benefit
set send_logs_level='none';

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(5e5);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_2'; -- stats available, but we have to recollect since data grew, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_3'; -- stats available, apply
set send_logs_level='none';

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(5e5 + 100000);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_4'; -- stats available, but we have to recollect since data grew, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_5'; -- stats available, apply
set send_logs_level='none';

TRUNCATE TABLE t;

INSERT INTO t SELECT toString(number), number FROM numbers(1e4);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_6'; -- stats available, but we have to recollect since data shrinked, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_7'; -- stats available, don't apply since no benefit
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

