-- The runtime-dataflow-statistics key of automatic parallel replicas hashes the actions DAGs of the plan
-- without the values of their variable-size constants, but with the fixed-size ones. Checks it on the
-- carriers it touches: a filter moved to PREWHERE (hashed by the read step) and a join.
--
-- The check, as in 04655: a query whose statistics are already cached does not collect them again
-- (`RuntimeDataflowStatisticsOutputBytes` stays 0). So the first run of a variant collects and its
-- repeat does not; the first run of a second variant that differs only in a constant must collect
-- again, unless the collision is an accepted one. The filters are on a column outside the primary
-- key, so index analysis reads the same rows for both variants and the row-count drift check cannot
-- hide a shared key by forcing a recollection.

SET enable_analyzer=1;
SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;
-- Keep the parallelized side oriented as written, and the plan identical between the collect and
-- apply runs (see 04655).
SET query_plan_join_swap_table='false';
SET query_plan_optimize_join_order_randomize=0, enable_join_runtime_filters=1, join_runtime_filter_min_probe_rows=1000;
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET max_threads=4, max_block_size=128;
SET use_query_condition_cache=0;
-- With unused columns kept, the plan with parallel replicas does not match the single-replica plan
-- for the subquery shapes below, and no statistics are collected at all.
SET query_plan_remove_unused_columns=1;

DROP TABLE IF EXISTS t_05292;
DROP TABLE IF EXISTS r_05292;
DROP TABLE IF EXISTS b_05292;

CREATE TABLE t_05292 (key UInt64, v UInt64, payload String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE r_05292 (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE b_05292 (x UInt32) ENGINE = MergeTree ORDER BY x;
-- A merge would change the read hash and force a recollection, which would mask a shared key.
SYSTEM STOP MERGES t_05292;
SYSTEM STOP MERGES r_05292;

INSERT INTO t_05292 SELECT number, number % 10, toString(cityHash64(number)) FROM numbers(25000);
INSERT INTO r_05292 SELECT number FROM numbers(25000);
INSERT INTO b_05292 SELECT number FROM numbers(1000);

-- A literal in a filter moved to PREWHERE.
SELECT payload FROM t_05292 WHERE v < 3 FORMAT Null SETTINGS log_comment='q05292_prewhere_3_0';
SELECT payload FROM t_05292 WHERE v < 3 FORMAT Null SETTINGS log_comment='q05292_prewhere_3_1';
SELECT payload FROM t_05292 WHERE v < 7 FORMAT Null SETTINGS log_comment='q05292_prewhere_7_0';

-- A constant passed through a subquery column is named `__table1.m`, not by its value. The value has
-- a fixed size, so it is hashed and the variants are still told apart.
SELECT payload FROM (SELECT payload, v, toUInt64(3) AS m FROM t_05292) WHERE v < m FORMAT Null SETTINGS log_comment='q05292_column_3_0';
SELECT payload FROM (SELECT payload, v, toUInt64(3) AS m FROM t_05292) WHERE v < m FORMAT Null SETTINGS log_comment='q05292_column_3_1';
SELECT payload FROM (SELECT payload, v, toUInt64(7) AS m FROM t_05292) WHERE v < m FORMAT Null SETTINGS log_comment='q05292_column_7_0';

-- A join whose right-side key carries a constant passed through a subquery column.
SELECT t1.payload FROM t_05292 AS t1 INNER JOIN (SELECT key, toUInt64(1) AS m FROM r_05292) AS t2 ON t1.key = t2.key + t2.m FORMAT Null SETTINGS log_comment='q05292_join_1_0';
SELECT t1.payload FROM t_05292 AS t1 INNER JOIN (SELECT key, toUInt64(1) AS m FROM r_05292) AS t2 ON t1.key = t2.key + t2.m FORMAT Null SETTINGS log_comment='q05292_join_1_1';
SELECT t1.payload FROM t_05292 AS t1 INNER JOIN (SELECT key, toUInt64(2) AS m FROM r_05292) AS t2 ON t1.key = t2.key + t2.m FORMAT Null SETTINGS log_comment='q05292_join_2_0';

-- An accepted collision: a scalar subquery with a `groupBitmap` state is named by the hash of the
-- subquery and its value has no fixed size, so the same query over changed data keeps its key and
-- reuses the statistics. A wrong match only costs a worse estimate.
SELECT payload FROM t_05292 WHERE bitmapContains((SELECT groupBitmapState(x) FROM b_05292), toUInt32(key)) FORMAT Null SETTINGS log_comment='q05292_heavy_before_0';
SELECT payload FROM t_05292 WHERE bitmapContains((SELECT groupBitmapState(x) FROM b_05292), toUInt32(key)) FORMAT Null SETTINGS log_comment='q05292_heavy_before_1';
INSERT INTO b_05292 SELECT number FROM numbers(1000, 1000);
SELECT payload FROM t_05292 WHERE bitmapContains((SELECT groupBitmapState(x) FROM b_05292), toUInt32(key)) FORMAT Null SETTINGS log_comment='q05292_heavy_changed_0';

DROP TABLE t_05292;
DROP TABLE r_05292;
DROP TABLE b_05292;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - toIntervalMinute(15))
  AND (current_database = currentDatabase()) AND (log_comment LIKE 'q05292\_%') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds
FORMAT TSVWithNames;
