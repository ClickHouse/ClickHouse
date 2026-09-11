-- Automatic parallel replicas only considers a query whose every plan step can collect dataflow
-- statistics, so a step that cannot takes every query containing it out of consideration. Lazy
-- materialization puts such a step in the plan: `LazilyReadFromMergeTree` reads the lazy columns
-- once the sort has picked the rows, under a `JoinLazyColumnsStep` that joins them back.
--
-- Both reads are executed by every replica - the plan the initiator ships is the whole query, so
-- each replica materializes its own rows lazily - so both belong in the statistics the cost model
-- prices the query with. The lazy read is the larger of the two by far: its rows are picked by the
-- sort and are spread over the whole table, so it touches almost every granule of those columns.
--
-- `automatic_parallel_replicas_mode = 2` collects statistics without ever switching to parallel
-- replicas, so the profile events below say whether the plan was considered at all. They are both
-- incremented by the single update that caches the collected statistics, which runs only when at
-- least one of them is non-zero, so their sum is exactly "the query was instrumented".

DROP TABLE IF EXISTS t_autopr_lazy_read;

-- Both columns are stored uncompressed so that their sizes do not depend on the server's compression
-- settings: `ord`, all the measured read keeps, is 800 KB, and `pad`, what the lazy read pulls, is
-- 50 MB. Wide parts because a compact part records no per-column sizes.
CREATE TABLE t_autopr_lazy_read (key UInt64, ord UInt64 CODEC(NONE), pad String CODEC(NONE))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_lazy_read SELECT number, cityHash64(number), repeat('x', 500) FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
-- Lazy materialization only applies to a `LIMIT` up to this value, and `clickhouse-test` randomizes
-- it down to 1, which would leave the plan without the step under test. Zero means no limit.
SET query_plan_max_limit_for_lazy_materialization = 0;

-- Ordering by a column outside the primary key rules out reading in order, which is what leaves the
-- top rows to be picked by a sort and the remaining columns to be read lazily afterwards.
SELECT count() FROM (EXPLAIN SELECT key, pad FROM t_autopr_lazy_read ORDER BY ord LIMIT 10000
    SETTINGS query_plan_optimize_lazy_materialization = 1)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT key, pad FROM t_autopr_lazy_read ORDER BY ord LIMIT 10000 FORMAT Null
SETTINGS query_plan_optimize_lazy_materialization = 1, log_comment = '05175_autopr_lazy_read_ordered';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- The second column is what says the lazy read was measured too. Everything the gate-side read keeps
-- is `ord`, 800 KB, so counting that alone lands two orders of magnitude below this bound, while
-- counting both lands near the 50 MB the query really reads.
SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 10000000 AS lazy_read_counted
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05175_autopr_lazy_read_ordered')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_lazy_read;
