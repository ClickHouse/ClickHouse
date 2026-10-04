-- Lazy materialization splits one read in two: the `ReadFromMergeTree` keeps the sorting column, and
-- a `LazilyReadFromMergeTree` reads the columns taken out of it. The lazy half is far the larger -
-- its rows are picked by the sort and are spread over the whole table, so it touches almost every
-- granule of those columns - but it is not what the gate sizes the plan by. `findReadingStep`
-- descends into the first child of `JoinLazyColumnsStep`, so the read the gate measures is the one
-- the optimization instruments and costs, and the only one it would parallelize.
--
-- So the size of the lazily read column must not decide the gate; the size of the scanned sorting
-- column must. Both tables below carry the same 150 MB of `pad` and differ only in `ord`.
--
-- `RuntimeDataflowStatisticsInputBytes` and `..OutputBytes` are both incremented by the single update
-- that caches the collected statistics, which runs only when at least one of them is non-zero, so
-- their sum says the gate let the query through and the optimization instrumented it.

DROP TABLE IF EXISTS t_autopr_lazy_small_scan;
DROP TABLE IF EXISTS t_autopr_lazy_large_scan;

-- `ord` is stored uncompressed in both so that its size does not depend on the server's compression
-- settings. 2.4 MB here, i.e. 800 KB per replica, below the 1 MiB threshold.
-- Wide parts: a compact part records no per-column sizes, so the estimate falls back to charging the
-- whole part - `pad` included - and would admit the read this case needs rejected. `clickhouse-test`
-- randomizes `min_bytes_for_wide_part` high enough to make these parts compact.
CREATE TABLE t_autopr_lazy_small_scan (key UInt64, ord UInt64 CODEC(NONE), pad String CODEC(NONE))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_lazy_small_scan SELECT number, cityHash64(number), repeat('x', 500) FROM numbers(300000);

-- Same lazily read `pad`, but `ord` is a wide string: 30 MB, i.e. 10 MB per replica, above it.
CREATE TABLE t_autopr_lazy_large_scan (key UInt64, ord String CODEC(NONE), pad String CODEC(NONE))
    ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_autopr_lazy_large_scan SELECT number, repeat(toString(cityHash64(number)), 5), repeat('x', 500) FROM numbers(300000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 1048576,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
-- Lazy materialization only applies to a `LIMIT` up to this value, and `clickhouse-test` randomizes
-- it down to 1, which would leave the plans without the step under test. Zero means no limit.
SET query_plan_max_limit_for_lazy_materialization = 0;
-- The gate declines to size any read while the range-split fault injection is armed, which would let
-- the rejected case through for that reason instead. `clickhouse-test` randomizes it.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Ordering by a column outside the primary key rules out reading in order, which is what leaves the
-- top rows to be picked by a sort and the remaining columns to be read lazily afterwards.
SELECT count() FROM (EXPLAIN SELECT key, pad FROM t_autopr_lazy_small_scan ORDER BY ord LIMIT 10000)
WHERE explain LIKE '%LazilyReadFromMergeTree%';
SELECT count() FROM (EXPLAIN SELECT key, pad FROM t_autopr_lazy_large_scan ORDER BY ord LIMIT 10000)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT key, pad FROM t_autopr_lazy_small_scan ORDER BY ord LIMIT 10000 FORMAT Null
SETTINGS log_comment = '05100_lazy_small_scan';
SELECT key, pad FROM t_autopr_lazy_large_scan ORDER BY ord LIMIT 10000 FORMAT Null
SETTINGS log_comment = '05100_lazy_large_scan';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment LIKE '05100_lazy_%')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_lazy_small_scan;
DROP TABLE t_autopr_lazy_large_scan;
