-- With `distributed_plan_execute_locally`, all tasks of a distributed plan run in this server at the same
-- time. The tasks of one stage share the thread limit their plan was built with (a subquery's own `SETTINGS`
-- included, lowered when free memory is short, never above `max_threads`): each runs on
-- max(1, limit / <tasks in the stage>) threads, and the single `main` task keeps the whole limit. Each query
-- reports, per task, the pipeline executor threads it ran on (none when it runs single-threaded), and whether
-- the thread pool of a parallel hash join followed the share (one thread, which builds and then clears).

DROP TABLE IF EXISTS t_share_threads;
CREATE TABLE t_share_threads (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO t_share_threads SELECT number, 1 FROM numbers(100000);
INSERT INTO t_share_threads SELECT number, 2 FROM numbers(100000);

-- A fuzzed re-run would inherit `log_comment` and be counted below.
SET ast_fuzzer_runs = 0;
-- A nonzero value makes the aggregation refuse to distribute.
SET max_rows_to_group_by = 0;
-- Free memory must not lower the thread limits below, except in the subquery that sets its own threshold.
SET max_threads_min_free_memory_per_thread = 0;
SET log_query_threads = 1;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

-- Every stage but `main` has as many tasks as the limit.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 4, max_threads = 4, log_comment = '05266_cascades';

-- 8 reading tasks get one thread each, 4 aggregating tasks two each.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 8, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 8, log_comment = '05266_rule_based';

-- A scalar subquery with its own SETTINGS is planned in that scope: its tasks share its max_threads, lower
-- or higher than the outer query's, and its limit lowered by a free-memory threshold the outer query lacks.
SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 4
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 16, log_comment = '05266_scoped_lower';

SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 8, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 8
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 2, log_comment = '05266_scoped_higher';

SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 8, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 8, max_threads_min_free_memory_per_thread = 1000000000000000000
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 8, log_comment = '05266_scoped_low_memory';

-- 8 reading tasks per side and 8 joining tasks get one thread each.
SELECT count() FROM t_share_threads AS a INNER JOIN t_share_threads AS b ON a.k = b.k FORMAT Null
SETTINGS enable_cascades_optimizer = 0, join_algorithm = 'parallel_hash', distributed_plan_default_reader_bucket_count = 8,
    distributed_plan_default_shuffle_join_bucket_count = 8, max_threads = 8, log_comment = '05266_parallel_hash';

-- A synchronous remote read raises the plan's limit to `max_distributed_connections`; the tasks share `max_threads`.
SELECT k % 1000 AS g, count() FROM remote('127.0.0.1', currentDatabase(), t_share_threads) GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 4, async_socket_for_remote = 0, max_distributed_connections = 64, prefer_localhost_replica = 1,
    log_comment = '05266_sync_remote';

SYSTEM FLUSH LOGS query_log, query_thread_log;

-- Per query: the pipeline executor threads of each of its tasks, sorted, and how many tasks started one or two
-- parallel hash join threads.
WITH initiators AS
(
    SELECT argMax(query_id, event_time_microseconds) AS query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND log_comment IN ('05266_cascades', '05266_rule_based', '05266_scoped_lower', '05266_scoped_higher',
            '05266_scoped_low_memory', '05266_parallel_hash', '05266_sync_remote')
        AND NOT match(query, '^(main|stage_\\d+_\\d+)$')
    GROUP BY log_comment
)
SELECT log_comment, arraySort(groupArray(pipeline_threads)), countIf(join_threads BETWEEN 1 AND 2)
FROM
(
    SELECT query_id, countIf(thread_name = 'QueryPipelineEx') AS pipeline_threads,
        countIf(thread_name = 'ConcurrentJoin') AS join_threads
    FROM system.query_thread_log
    WHERE event_date >= yesterday() AND query_id IN (SELECT query_id FROM initiators)
    GROUP BY query_id, master_thread_id
    HAVING countIf(thread_name = 'DistQueryTask') = 1
) AS tasks
INNER JOIN initiators USING (query_id)
GROUP BY log_comment
ORDER BY log_comment
SETTINGS make_distributed_plan = 0;

DROP TABLE t_share_threads;
