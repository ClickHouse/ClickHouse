-- With `distributed_plan_execute_locally`, all tasks of a distributed plan run in this server at the same
-- time. The tasks of one stage share the thread limit their plan was built with (a subquery's own
-- `SETTINGS` included, lowered when free memory is short, never above `max_threads`): each gets
-- max(1, limit / <tasks in the stage>), and a stage with a single task keeps all of it.

DROP TABLE IF EXISTS t_share_threads;
CREATE TABLE t_share_threads (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO t_share_threads SELECT number, 1 FROM numbers(100000);
INSERT INTO t_share_threads SELECT number, 2 FROM numbers(100000);

-- A fuzzed re-run would inherit `log_comment` and be counted below.
SET ast_fuzzer_runs = 0;
-- A nonzero value makes the aggregation refuse to distribute.
SET max_rows_to_group_by = 0;
-- Free memory must not lower the thread limits below, except in the query that sets its own threshold.
SET max_threads_min_free_memory_per_thread = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 4, max_threads = 16,
    log_comment = '05266_cascades';

-- Three reading tasks and 32 aggregating tasks.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 32,
    max_threads = 16, log_comment = '05266_rule_based';

-- A scalar subquery with its own SETTINGS is planned in that scope: its tasks share its max_threads,
-- whether it is lower or higher than the outer query's.
SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 4
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 16, log_comment = '05266_scoped_lower';

SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 16
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 2, log_comment = '05266_scoped_higher';

-- Less free memory than this per thread lowers the plan's limit to 1, which its tasks share.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 16, max_threads_min_free_memory_per_thread = 1000000000000000000, log_comment = '05266_low_memory';

-- A synchronous remote read raises the plan's limit to `max_distributed_connections`; the tasks share `max_threads`.
SELECT k % 1000 AS g, count() FROM remote('127.0.0.1', currentDatabase(), t_share_threads) GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 16, async_socket_for_remote = 0, max_distributed_connections = 64, prefer_localhost_replica = 1,
    log_comment = '05266_sync_remote';

SYSTEM FLUSH LOGS query_log;

-- Per query: every task got its stage's share, the widest stage, what the single `main` task kept.
SELECT log_comment, min(ok), max(tasks), anyIf(value, stage = 'main')
FROM
(
    SELECT log_comment, stage, uniqExact(task) AS tasks, any(max_threads) AS value,
        groupUniqArray(max_threads) = [toString(greatest(1, intDiv(
            multiIf(log_comment = '05266_scoped_lower', 4, log_comment = '05266_low_memory', 1, 16), tasks)))] AS ok
    FROM
    (
        SELECT log_comment, query AS task, replaceRegexpOne(query, '_\\d+$', '') AS stage, Settings['max_threads'] AS max_threads
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND match(query, '^(main|stage_\\d+_\\d+)$')
            AND initial_query_id IN (
                SELECT argMax(query_id, event_time_microseconds)
                FROM system.query_log
                WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
                    AND log_comment IN ('05266_cascades', '05266_rule_based', '05266_scoped_lower', '05266_scoped_higher',
                        '05266_low_memory', '05266_sync_remote')
                    AND NOT match(query, '^(main|stage_\\d+_\\d+)$')
                GROUP BY log_comment)
    )
    GROUP BY log_comment, stage
)
GROUP BY log_comment
ORDER BY log_comment
SETTINGS make_distributed_plan = 0;

DROP TABLE t_share_threads;
