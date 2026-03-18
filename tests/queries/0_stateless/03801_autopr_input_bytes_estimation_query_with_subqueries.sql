-- Tags: stateful

-- To avoid too slow test execution
set remote_filesystem_read_method='threadpool', allow_prefetched_read_pool_for_remote_filesystem=1, filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes='100Mi';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- External aggregation is not supported as of now
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

SET use_query_condition_cache=0;

create table t(a UInt64) engine=MergeTree order by a;
insert into t select number from numbers_mt(1e6);

SELECT a % 10000 FROM t FORMAT Null SETTINGS log_comment='03801_autopr_input_bytes_estimation_query_with_subqueries_query_0';

-- `CounterID` is part of the PK
SELECT EventTime, CounterID, URL, Referer FROM test.hits WHERE CounterID IN (SELECT a % 10000 FROM t) FORMAT Null SETTINGS log_comment='03801_autopr_input_bytes_estimation_query_with_subqueries_query_1';
-- `WatchID` is not
SELECT EventTime, CounterID, URL, Referer FROM test.hits WHERE WatchID IN (SELECT a % 10000 FROM t) FORMAT Null SETTINGS log_comment='03801_autopr_input_bytes_estimation_query_with_subqueries_query_2';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off
--
-- We subtract the compressed bytes of the subquery because it cannot be executed with parallel replicas in the current infrastructure,
-- so the "parallelizable" part of the query is only the main query itself, thus AutoPR heuristic should use only its estimation.
WITH (
    SELECT
        ProfileEvents['ReadCompressedBytes']
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (log_comment = '03801_autopr_input_bytes_estimation_query_with_subqueries_query_0') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
) AS compressed_bytes_subquery
SELECT format('{} {} {}', log_comment, compressed_bytes, statistics_input_bytes)
FROM (
    SELECT
        log_comment,
        ProfileEvents['ReadCompressedBytes'] - compressed_bytes_subquery AS compressed_bytes,
        ProfileEvents['RuntimeDataflowStatisticsInputBytes']::Int64 statistics_input_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES') AND (current_database = currentDatabase()) AND (match(log_comment, '03801_autopr_input_bytes_estimation_query_with_subqueries_query_[12]')) AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE greatest(compressed_bytes, statistics_input_bytes) / least(compressed_bytes, statistics_input_bytes) > 2;
