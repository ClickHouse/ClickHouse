-- Tags: no-sanitizers, long, no-parallel
-- no-sanitizers: too slow
-- long: for flaky check
-- no-parallel: Depends on the query condition cache content (queries executed in parallel may overflow the cache size or straight away call "clear cache")

DROP TABLE IF EXISTS t;

-- index_granularity: to be able to produce small blocks from reading
CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity=128;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- For runs with the old analyzer
SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

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
set send_logs_level='none';

-- Query #2 will see a lot of bytes read, since we read all rows.
-- At the same time, as query #2 completes, it populates the query condition cache with an entry for the condition "value = 42".
-- Subsequent queries will read much less data and thus the automatic parallel replicas decision should change.
-- The goal is to verify that it will actually happen.
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_2'; -- empty cache, don't apply optimization, collect stats

-- For an unclear reason, the QCC is not always populated with the knowledge about all irrelevant granules from the first run.
-- It may take a few invocations (we do five to be sure). Because of that, we do a bit relaxed check here and only check the end result - that parallel replicas won't be used eventually.
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null;
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null;
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null;
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null;
SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null;

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_3'; -- stats available, don't apply since no benefit

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

