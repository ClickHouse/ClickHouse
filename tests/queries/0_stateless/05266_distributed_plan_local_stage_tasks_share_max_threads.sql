-- With `distributed_plan_execute_locally`, all tasks of a distributed plan run in this server at the same
-- time. The tasks of one stage share the query's `max_threads`: each gets max(1, max_threads / <tasks in
-- the stage>), and a stage with a single task keeps all of it.

DROP TABLE IF EXISTS t_share_threads;
CREATE TABLE t_share_threads (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO t_share_threads SELECT number, 1 FROM numbers(100000);
INSERT INTO t_share_threads SELECT number, 2 FROM numbers(100000);

-- A fuzzed re-run would inherit `log_comment` and be counted below.
SET ast_fuzzer_runs = 0;
-- A nonzero value makes the aggregation refuse to distribute.
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 4, max_threads = 16,
    log_comment = '05266_cascades';

-- Three reading tasks and 256 aggregating tasks.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 3, distributed_plan_default_shuffle_join_bucket_count = 256,
    max_threads = 16, log_comment = '05266_rule_based';

SYSTEM FLUSH LOGS query_log;

-- Per query: every task got its stage's share, the widest stage, what the single `main` task kept.
SELECT log_comment, min(ok), max(tasks), anyIf(value, stage = 'main')
FROM
(
    SELECT log_comment, stage, count() AS tasks, any(max_threads) AS value,
        groupUniqArray(max_threads) = [toString(greatest(1, intDiv(16, tasks)))] AS ok
    FROM
    (
        SELECT log_comment, replaceRegexpOne(query, '_\\d+$', '') AS stage, Settings['max_threads'] AS max_threads
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND match(query, '^(main|stage_\\d+_\\d+)$')
            AND initial_query_id IN (
                SELECT argMax(query_id, event_time_microseconds)
                FROM system.query_log
                WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
                    AND log_comment IN ('05266_cascades', '05266_rule_based') AND NOT match(query, '^(main|stage_\\d+_\\d+)$')
                GROUP BY log_comment)
    )
    GROUP BY log_comment, stage
)
GROUP BY log_comment
ORDER BY log_comment
SETTINGS make_distributed_plan = 0;

DROP TABLE t_share_threads;
