-- `automatic_parallel_replicas_min_bytes_per_replica` turns a query away at two points, each with its
-- own event, and both record the size they judged in `AutoParallelReplicasBytesPerReplica`:
--  * before building the plan with parallel replicas, from the on-disk size of the largest read
--    (`AutoParallelReplicasSkippedEarly`);
--  * after the cost model favoured parallel replicas, from the bytes the parallelized read actually
--    read (`AutoParallelReplicasRejectedByThreshold`).
--
-- The second needs a read that looks large on disk but reads little: `PREWHERE` matches one row, so
-- only its granule of `payload` is read, while the early estimate counts the whole column.

DROP TABLE IF EXISTS t_autopr_min_bytes;

-- Wide parts, so that the early estimate knows the size of every column; small granules, so that the
-- one matching row reads little of `payload`.
CREATE TABLE t_autopr_min_bytes (key UInt64, flag UInt8, payload String) ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_autopr_min_bytes
SELECT number, number = 50000,
       -- 256 hex characters that hardly compress; `sipHash128` because builds without SSL have no `SHA256`.
       concat(hex(sipHash128(number, 1)), hex(sipHash128(number, 2)), hex(sipHash128(number, 3)), hex(sipHash128(number, 4)),
              hex(sipHash128(number, 5)), hex(sipHash128(number, 6)), hex(sipHash128(number, 7)), hex(sipHash128(number, 8)))
FROM numbers(100000);

SET enable_analyzer = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- Let the cost model favour parallel replicas for a read this small: it caps the reading threads by
-- `merge_tree_min_bytes_per_task_for_remote_reading`, which would otherwise make the two plans equal.
SET max_threads = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1;
-- With the range-splitting fault injection armed, the early estimate declines to answer, and a read of
-- unknown size is never turned away.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Skipped early: no read in the plan comes near the threshold.
SELECT key % 10 AS k, max(payload) FROM t_autopr_min_bytes PREWHERE flag = 1 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_min_bytes_per_replica = 1000000000000, log_comment = 'autopr_min_bytes_skipped_early';

-- Rejected by the threshold: the early estimate clears 500 KB per replica, the bytes actually read do not.
-- Twice: the first run only collects statistics, the second is the one the cost model decides.
SELECT key % 10 AS k, max(payload) FROM t_autopr_min_bytes PREWHERE flag = 1 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_min_bytes_per_replica = 500000, log_comment = 'autopr_min_bytes_collect';
SELECT key % 10 AS k, max(payload) FROM t_autopr_min_bytes PREWHERE flag = 1 GROUP BY k FORMAT Null
SETTINGS automatic_parallel_replicas_min_bytes_per_replica = 500000, log_comment = 'autopr_min_bytes_rejected';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasSkippedEarly'] AS skipped_early,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS collected,
       ProfileEvents['AutoParallelReplicasCostModelEvaluated'] AS evaluated,
       ProfileEvents['AutoParallelReplicasRejectedByThreshold'] AS rejected_by_threshold,
       ProfileEvents['AutoParallelReplicasApplied'] AS applied,
       ProfileEvents['AutoParallelReplicasBytesPerReplica'] > 0 AS size_recorded,
       ProfileEvents['AutoParallelReplicasBytesPerReplica'] < Settings['automatic_parallel_replicas_min_bytes_per_replica']::UInt64 AS size_below_threshold
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND startsWith(log_comment, 'autopr_min_bytes_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY event_time_microseconds;

DROP TABLE t_autopr_min_bytes;
