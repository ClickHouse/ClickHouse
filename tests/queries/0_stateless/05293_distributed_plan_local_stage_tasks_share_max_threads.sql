-- With `distributed_plan_execute_locally`, the tasks of one stage share the thread limit their plan was built
-- with: each runs on max(1, limit / <tasks in the stage>) threads. Per query, the pipeline executor threads of
-- each task (none when it runs single-threaded).

DROP TABLE IF EXISTS t_share_threads;
CREATE TABLE t_share_threads (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO t_share_threads SELECT number, 1 FROM numbers(100000);
INSERT INTO t_share_threads SELECT number, 2 FROM numbers(100000);

DROP TABLE IF EXISTS t_share_merge;
CREATE TABLE t_share_merge (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO t_share_merge SELECT number FROM numbers(200000);

-- A fuzzed re-run would inherit `log_comment` and be counted below.
SET ast_fuzzer_runs = 0;
-- A nonzero value makes the aggregation refuse to distribute.
SET max_rows_to_group_by = 0;
SET max_threads_min_free_memory_per_thread = 0;
SET log_query_threads = 1;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

-- 8 reading tasks get one thread each, 4 aggregating tasks two each.
SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 8, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 8, log_comment = '05293_rule_based';

-- A subquery with its own SETTINGS is planned in that scope, so its tasks share its max_threads.
SELECT (
    SELECT count() FROM (SELECT k % 1000 AS g, count() FROM t_share_threads GROUP BY g)
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
        distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4,
        max_threads = 4
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 16, log_comment = '05293_scoped_lower';

-- A synchronous remote read raises the plan's limit to `max_distributed_connections`; the tasks share `max_threads`.
SELECT k % 1000 AS g, count() FROM remote('127.0.0.1', currentDatabase(), t_share_threads) GROUP BY g FORMAT Null
SETTINGS enable_cascades_optimizer = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_default_reader_bucket_count = 4, distributed_plan_default_shuffle_join_bucket_count = 4,
    max_threads = 4, async_socket_for_remote = 0, max_distributed_connections = 64, prefer_localhost_replica = 1,
    log_comment = '05293_sync_remote';

-- `MergingAggregatedStep` sizes its aggregator pool from the task's settings while the fragment is deserialized.
-- The merge of the grouping sets runs in one task with the subquery's limit of 2 and gets 200,000 two-level
-- partial states, which its aggregator merges and then converts with two threads each.
SELECT (
    SELECT count() FROM (SELECT k, count() FROM t_share_merge GROUP BY GROUPING SETS ((k), ()))
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_default_reader_bucket_count = 8,
        distributed_plan_default_shuffle_join_bucket_count = 8, max_threads = 2, group_by_two_level_threshold = 1,
        max_bytes_before_external_group_by = 10000000000, max_bytes_ratio_before_external_group_by = 0,
        optimize_aggregation_in_order = 0
) FORMAT Null
SETTINGS make_distributed_plan = 0, max_threads = 8, log_comment = '05293_merge_pool';

SYSTEM FLUSH LOGS query_log, query_thread_log;

WITH initiators AS
(
    SELECT argMax(query_id, event_time_microseconds) AS query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND log_comment IN ('05293_rule_based', '05293_scoped_lower', '05293_sync_remote')
        AND NOT match(query, '^(main|stage_\\d+_\\d+)$')
    GROUP BY log_comment
)
SELECT log_comment, arraySort(groupArray(pipeline_threads))
FROM
(
    -- A task runs under its own query id; `initial_query_id` is the initiator's.
    SELECT initial_query_id AS query_id, countIf(thread_name = 'QueryPipelineEx') AS pipeline_threads
    FROM system.query_thread_log
    WHERE event_date >= yesterday() AND initial_query_id IN (SELECT query_id FROM initiators)
    GROUP BY initial_query_id, query_id, master_thread_id
    HAVING countIf(thread_name = 'DistQueryTask') = 1
) AS tasks
INNER JOIN initiators USING (query_id)
GROUP BY log_comment
ORDER BY log_comment
SETTINGS make_distributed_plan = 0;

-- The aggregator pool threads of each task of the grouping-sets query that started any.
SELECT arrayFilter(x -> x > 0, groupArray(agg_threads))
FROM
(
    SELECT countIf(thread_name = 'AggregatorPool') AS agg_threads
    FROM system.query_thread_log
    WHERE event_date >= yesterday() AND initial_query_id =
    (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND log_comment = '05293_merge_pool' AND NOT match(query, '^(main|stage_\\d+_\\d+)$')
    )
    GROUP BY query_id, master_thread_id
    HAVING countIf(thread_name = 'DistQueryTask') = 1
)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_share_threads;
DROP TABLE t_share_merge;
