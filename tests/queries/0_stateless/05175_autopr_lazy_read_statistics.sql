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

-- A lazy read reached only through the join branch that is not parallelized is a different table:
-- every replica reads that side in full instead of splitting it, while the cost model divides
-- `input_bytes` by the number of replicas. It must therefore stay out of these statistics, even
-- though it is by far the largest read in the query.
DROP TABLE IF EXISTS t_autopr_lazy_read_other;

CREATE TABLE t_autopr_lazy_read_other (key UInt64, ord UInt64 CODEC(NONE), pad String CODEC(NONE))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_lazy_read_other SELECT number, cityHash64(number), repeat('x', 500) FROM numbers(100000);

-- The assertion below is about which side of the join is instrumented, so pin the two settings that
-- decide it. `clickhouse-test` randomizes both: a randomized join order makes the probe plan and this
-- plan pick different orders, and their hashes then no longer match, which takes the query out of
-- consideration entirely; a swap would move the parallelized read to the other table.
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'false';

-- The lazily materialized subquery is the smaller side, so the join keeps it on the right and the
-- parallelized read is `t_autopr_lazy_read`, whose only column here is `key`.
SELECT count() FROM (EXPLAIN SELECT a.key, b.pad FROM t_autopr_lazy_read AS a
    INNER JOIN (SELECT key, pad FROM t_autopr_lazy_read_other ORDER BY ord LIMIT 10) AS b ON a.key = b.key
    SETTINGS query_plan_optimize_lazy_materialization = 1)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT a.key, b.pad FROM t_autopr_lazy_read AS a
    INNER JOIN (SELECT key, pad FROM t_autopr_lazy_read_other ORDER BY ord LIMIT 10) AS b ON a.key = b.key
FORMAT Null
SETTINGS query_plan_optimize_lazy_materialization = 1, log_comment = '05175_autopr_lazy_read_join';

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
    AND (type = 'QueryFinish') AND is_initial_query
-- One row per run of this test. Re-running it inside the same database - as the flaky check does -
-- would otherwise leave the earlier run's row inside the time window and emit it too.
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The parallelized read keeps only `key`, under a megabyte, while the lazy read of the other table is
-- tens of megabytes, so the same bound as above separates the two, this time the other way round.
SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes'] < 10000000 AS other_table_lazy_read_not_counted
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05175_autopr_lazy_read_join')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_autopr_lazy_read;
DROP TABLE t_autopr_lazy_read_other;
