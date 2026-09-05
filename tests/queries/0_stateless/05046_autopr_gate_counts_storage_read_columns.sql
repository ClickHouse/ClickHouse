-- The check that skips building the parallel-replicas plan sizes a read by the columns the storage
-- actually reads, not by the ones the step projects. A `PREWHERE` column need not be projected at
-- all, and it is usually the largest thing the query touches, so pricing only the projected columns
-- would reject reads that are in fact well above
-- `automatic_parallel_replicas_min_bytes_per_replica`.

DROP TABLE IF EXISTS t_prewhere;

-- `small` is the only projected column and is far below the threshold. `bignum` is read by PREWHERE
-- and is stored uncompressed so that its size does not depend on the server's compression settings:
-- 24 MB, i.e. 8 MB per replica against the 1 MiB threshold below.
CREATE TABLE t_prewhere(k UInt64, small UInt8, bignum UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_prewhere SELECT number, number % 251, number * 7 FROM numbers(3e6);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET automatic_parallel_replicas_min_bytes_per_replica=1048576;
SET optimize_move_to_prewhere=1;

-- The read is worth distributing because of the PREWHERE column, so the plan is built and statistics
-- are collected. Sizing this read by `small` alone would give 8 KB per replica and reject it.
SELECT count() FROM t_prewhere PREWHERE bignum > 0 GROUP BY small FORMAT Null SETTINGS log_comment='05046_gate_prewhere';

DROP TABLE IF EXISTS t_ordered;

-- The whole sorting key is read when the read is ordered, not just the prefix the query orders by,
-- so `bigkey` is read even though the query neither projects it nor orders by it. It is stored
-- uncompressed so that its size does not depend on the server's compression settings.
CREATE TABLE t_ordered(k1 UInt32, bigkey UInt64 CODEC(NONE), small UInt8) ENGINE = MergeTree ORDER BY (k1, bigkey)
    SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO t_ordered SELECT number % 1000, number * 7, number % 251 FROM numbers(3e6);

-- Sizing this read by the projected `small` and the ordered-by `k1` alone would give 38 KB per
-- replica and reject it; with the sorting key included it is 8 MB per replica.
SELECT small FROM t_ordered ORDER BY k1 LIMIT 10 FORMAT Null SETTINGS log_comment='05046_gate_ordered', optimize_read_in_order=1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05046_gate_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_prewhere;
DROP TABLE t_ordered;
