-- Automatic parallel replicas only considers a query whose every plan step can collect dataflow
-- statistics, so a step that cannot takes every query containing it out of consideration. Lazy
-- materialization puts such a step in the plan: `LazilyReadFromMergeTree` reads the lazy columns
-- once the sort has picked the rows, under a `JoinLazyColumnsStep` that joins them back.
--
-- `automatic_parallel_replicas_mode = 2` collects statistics without ever switching to parallel
-- replicas, so the profile events below say whether the plan was considered at all. They are both
-- incremented by the single update that caches the collected statistics, which runs only when at
-- least one of them is non-zero, so their sum is exactly "the query was instrumented".

DROP TABLE IF EXISTS t_autopr_lazy_read;

CREATE TABLE t_autopr_lazy_read (key UInt64, ord UInt64, pad String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_autopr_lazy_read SELECT number, cityHash64(number), repeat('x', 100) FROM numbers(200000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
-- Lazy materialization only applies to a `LIMIT` up to this value, and `clickhouse-test` randomizes
-- it down to 1, which would leave the plan without the step under test. Zero means no limit.
SET query_plan_max_limit_for_lazy_materialization = 0;

-- Ordering by a column outside the primary key rules out reading in order, which is what leaves the
-- top rows to be picked by a sort and the remaining columns to be read lazily afterwards.
SELECT count() FROM (EXPLAIN SELECT key, pad FROM t_autopr_lazy_read ORDER BY ord LIMIT 10
    SETTINGS query_plan_optimize_lazy_materialization = 1)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT key, pad FROM t_autopr_lazy_read ORDER BY ord LIMIT 10 FORMAT Null
SETTINGS query_plan_optimize_lazy_materialization = 1, log_comment = '05175_autopr_lazy_read_ordered';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05175_autopr_lazy_read_ordered')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_lazy_read;
