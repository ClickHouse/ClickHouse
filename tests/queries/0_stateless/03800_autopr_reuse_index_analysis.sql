-- Tags: stateful, long

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- To avoid too slow test execution
set remote_filesystem_read_method='threadpool', allow_prefetched_read_pool_for_remote_filesystem=1, filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes='100Mi';

SET automatic_parallel_replicas_min_bytes_per_replica=0;

SET use_query_condition_cache=0;

-- Check that index analysis is performed only once in both cases: when we choose local plan and when we choose parallel replicas

-- Pre-warm the cache
SELECT URL FROM test.hits WHERE UserID >= 730800628386 FORMAT Null;

SELECT sum(length(URL)) FROM test.hits WHERE UserID >= 730800628386 FORMAT Null;

-- Local plan wins
SELECT URL FROM test.hits WHERE UserID >= 730800628386 FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_1';

-- Parallel replicas plan wins
SELECT sum(length(URL)) FROM test.hits WHERE UserID >= 730800628386 FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_2';

-- A case when subquery is present

create table t(a UInt64) engine=MergeTree order by a;
insert into t select number from numbers_mt(1e6);

-- Pre-warm the cache
SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) FORMAT Null;

SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) OR UserID IN (SELECT a % 1000000 FROM t) FORMAT Null;

SELECT sum(length(URL)) FROM test.hits WHERE WatchID IN (SELECT a % 10000000 FROM t) FORMAT Null;

SELECT sum(length(URL))
FROM test.hits
WHERE WatchID IN (SELECT a % 1000000 FROM t)
FORMAT Null;

SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_3';

SELECT sum(length(URL)) FROM test.hits WHERE CounterID IN (SELECT a % 100000 FROM t) OR UserID IN (SELECT a % 1000000 FROM t) FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_4';

SELECT sum(length(URL)) FROM test.hits WHERE WatchID IN (SELECT a % 1000000 FROM t) FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_5';

-- For Global IN-s now we can execute subquery twice with automatic parallel replicas :(
SELECT sum(length(URL)) FROM test.hits WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t) FORMAT Null SETTINGS log_comment='3800_autopr_reuse_index_analysis_query_6';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['IndexAnalysisRounds'] index_analysis_rounds
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '3800_autopr_reuse_index_analysis_query_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds;

