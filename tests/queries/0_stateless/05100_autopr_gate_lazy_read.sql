-- The gate that skips building the parallel-replicas plan sizes a read from index analysis, and lazy
-- materialization splits one read into two: the step the gate measures keeps only the sorting column,
-- and a `LazilyReadFromMergeTree` reads the columns that were taken out of it. Which rows that second
-- read touches is decided by the sort at execution, so the gate cannot size it - and it is not small:
-- the top rows of an unordered sort are spread over the whole table, so it reads almost every granule
-- of those columns. Measured on the table below: the gate sized the read at 2,400,924 bytes while the
-- query read 156,007,849, a 65x under-count that rejected a query reading 52 MB per replica.
--
-- `RuntimeDataflowStatisticsInputBytes` and `..OutputBytes` are both incremented by the single update
-- that caches the collected statistics, which runs only when at least one of them is non-zero, so
-- their sum says the gate let the query through and the optimization instrumented it.

DROP TABLE IF EXISTS t_autopr_gate_lazy;

-- `ord` is the only column the measured read keeps, and it is stored uncompressed so that its size
-- does not depend on the server's compression settings: 2.4 MB, i.e. 800 KB per replica, below the
-- 1 MiB threshold. `pad` is what the lazy read pulls, and it is 60x larger.
CREATE TABLE t_autopr_gate_lazy (key UInt64, ord UInt64 CODEC(NONE), pad String CODEC(NONE))
    ENGINE = MergeTree ORDER BY key;

INSERT INTO t_autopr_gate_lazy SELECT number, cityHash64(number), repeat('x', 500) FROM numbers(300000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 1048576,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
-- Lazy materialization only applies to a `LIMIT` up to this value, and `clickhouse-test` randomizes
-- it down to 1, which would leave the plan without the step under test. Zero means no limit.
SET query_plan_max_limit_for_lazy_materialization = 0;
-- The gate declines to size any read while the range-split fault injection is armed, which would let
-- this query through for that reason instead of the one under test. `clickhouse-test` randomizes it.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Ordering by a column outside the primary key rules out reading in order, which is what leaves the
-- top rows to be picked by a sort and the remaining columns to be read lazily afterwards.
SELECT count() FROM (EXPLAIN SELECT key, pad FROM t_autopr_gate_lazy ORDER BY ord LIMIT 10000)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT key, pad FROM t_autopr_gate_lazy ORDER BY ord LIMIT 10000 FORMAT Null
SETTINGS log_comment = '05100_autopr_gate_lazy_read';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05100_autopr_gate_lazy_read')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_gate_lazy;
