-- `join_any_take_last_row` selects the last instead of the first matching right-side row for `ANY`
-- joins, so the two modes can collect very different dataflow statistics for the same query text.
-- They must therefore not share a runtime-dataflow-statistics cache entry.
--
-- The check: a query whose statistics are already cached does not collect them again
-- (`RuntimeDataflowStatisticsOutputBytes` stays 0). So the first run of each mode must collect, and
-- the repeat of that same mode must not. If both modes shared one key, the first run under
-- `join_any_take_last_row = 1` would reuse the entry collected under `= 0` and report 0.

-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET parallel_replicas_prefer_local_join=1;

-- Keep the parallelized side oriented as written (the randomizer may flip this).
SET query_plan_join_swap_table='false';

SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET merge_tree_min_bytes_per_task_for_remote_reading=0;
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET max_threads=4, max_block_size=128;
SET use_query_condition_cache=0;

DROP TABLE IF EXISTS atlr_left;
DROP TABLE IF EXISTS atlr_right;

CREATE TABLE atlr_left (key UInt64, payload String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;
CREATE TABLE atlr_right (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=128;

-- A merge would change the read hash and force a recollection, which would mask a shared key.
SYSTEM STOP MERGES atlr_left;
SYSTEM STOP MERGES atlr_right;

INSERT INTO atlr_left SELECT number, toString(cityHash64(number)) FROM numbers(25000);
-- Duplicate keys on the right, so `join_any_take_last_row` actually picks a different row.
INSERT INTO atlr_right SELECT number % 5000 FROM numbers(10000);

-- First run of each mode: cache miss, statistics are collected.
-- Repeat of the same mode: cache hit, nothing is collected.
SELECT t1.payload FROM atlr_left AS t1 ANY LEFT JOIN atlr_right AS t2 USING (key) FORMAT Null
SETTINGS join_any_take_last_row=0, log_comment='04655_any_first_row_0';
SELECT t1.payload FROM atlr_left AS t1 ANY LEFT JOIN atlr_right AS t2 USING (key) FORMAT Null
SETTINGS join_any_take_last_row=0, log_comment='04655_any_first_row_1';
SELECT t1.payload FROM atlr_left AS t1 ANY LEFT JOIN atlr_right AS t2 USING (key) FORMAT Null
SETTINGS join_any_take_last_row=1, log_comment='04655_any_last_row_0';
SELECT t1.payload FROM atlr_left AS t1 ANY LEFT JOIN atlr_right AS t2 USING (key) FORMAT Null
SETTINGS join_any_take_last_row=1, log_comment='04655_any_last_row_1';

DROP TABLE atlr_left;
DROP TABLE atlr_right;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment AS query, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS stats_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - toIntervalMinute(15))
  AND (current_database = currentDatabase()) AND (log_comment LIKE '04655_any_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;
