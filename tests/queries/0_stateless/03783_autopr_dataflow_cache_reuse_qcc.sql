-- Tags: no-parallel
-- Tag no-parallel: Depends on the query condition cache content (queries executed in parallel may overflow the cache size or straight away call "clear cache")

DROP TABLE IF EXISTS t;

-- TODO(nickitat): Enforcing wide parts is a temporary workaround, not sure why collection doesn't work otherwise
CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=8192, min_bytes_for_wide_part=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=2;

-- For runs with the old analyzer
SET enable_analyzer=1;

SET use_query_condition_cache=1;
SET automatic_parallel_replicas_min_bytes_per_replica='1Mi';

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(1e6);

-- Try to avoid eviction of relevant cache entries
SYSTEM CLEAR QUERY CONDITION CACHE;

--set send_logs_level='trace', send_logs_source_regexp = 'optimize|SelectExecutor';
SELECT SUM(value) FROM t FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_0'; -- empty cache, don't apply optimization, collect stats

SELECT SUM(value) FROM t FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_1'; -- stats available, apply

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_2'; -- empty cache, don't apply optimization, collect stats

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_3'; -- stats available, but we have to recollect since data shrinked (due to pruning by QCC), don't apply

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_4'; -- stats available, don't apply since no benefit
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

