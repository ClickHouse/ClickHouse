-- The gate that skips building the parallel-replicas plan prices a read by the columns index
-- analysis says it reads. `spreadMarkRangesAmongStreams` can invalidate that: with the range-split
-- fault injection armed it turns an ordinary read into an in-order one and appends the whole sorting
-- key to the columns read. The decision is a coin flip made when the pipeline is built, long after
-- the gate runs, so the gate must not size such a read at all.
--
-- The sorting key here is 530x the projected column, so pricing the read without it rejects a query
-- whose real read is far above the threshold. `clickhouse-test` randomizes the injection setting, so
-- without this the gate would reject reads at random in CI.
--
-- `RuntimeDataflowStatisticsInputBytes` and `..OutputBytes` are both incremented by the single
-- update that caches the collected statistics, which runs only when at least one of them is
-- non-zero, so their sum says the gate let the query through and the optimization instrumented it.

DROP TABLE IF EXISTS t_autopr_range_split;

-- `small` is the only projected column and is far below the threshold. `bigkey` is in the sorting
-- key and is stored uncompressed so that its size does not depend on the server's compression
-- settings: 24 MB, i.e. 8 MB per replica against the 1 MiB threshold below.
CREATE TABLE t_autopr_range_split(k1 UInt32, bigkey UInt64 CODEC(NONE), small UInt8)
    ENGINE = MergeTree ORDER BY (k1, bigkey)
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_autopr_range_split SELECT number % 1000, number * 7, number % 251 FROM numbers(3e6);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 1048576,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;
SET max_threads = 4;

-- A probability of 1 makes the injection fire on every execution. The read must also be plain for
-- `spreadMarkRangesAmongStreams` to take that path: no PREWHERE, no query condition cache, more than
-- one stream. Sizing it by `small` alone gives 15 KB per replica and rejects it.
SELECT small FROM t_autopr_range_split FORMAT Null
SETTINGS log_comment = '05099_autopr_gate_range_split_injection',
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1,
    use_query_condition_cache = 0, optimize_read_in_order = 0;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05099_autopr_gate_range_split_injection')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_range_split;
