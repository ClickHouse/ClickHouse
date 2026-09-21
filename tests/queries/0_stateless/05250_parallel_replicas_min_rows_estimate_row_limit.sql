-- The replica-count estimate taken when parallel_replicas_min_number_of_rows_per_replica is set
-- must not enforce the throwing read row limits: it charges whole granules of the base table, while
-- the read that executes may be served by a projection or bounded by its read order.

SET enable_analyzer = 1;
SET use_statistics_for_part_pruning = 0;
SET automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 2, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_min_number_of_rows_per_replica = 1;

DROP TABLE IF EXISTS x;
CREATE TABLE x (i int) ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 3;
SYSTEM STOP MERGES x;
INSERT INTO x SELECT number FROM numbers(10);

SELECT '--- implicit projection serves the query within the budget ---';
-- A threshold the 10-row table cannot meet reaches the same block's silent decline, so this pair is
-- what shows the estimate ran at all: skip the block and this query engages parallel replicas like
-- any other, flipping the 0 below to 1 while every other line stays green.
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1,
    parallel_replicas_min_number_of_rows_per_replica = 1000, log_comment = '05250_declined_pr';
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1, log_comment = '05250_projection_pr';
SELECT max(i) FROM x SETTINGS max_rows_to_read_leaf = 2, optimize_use_implicit_projections = 1;

-- Parallel replicas really engaged for the shape under test, and really declined for the paired
-- threshold, so both propositions are observed rather than assumed. The counter is incremented
-- when the reading coordinator is destroyed.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMax(ProfileEvents['ParallelReplicasQueryCount'], event_time_microseconds) > 0 AS parallel_replicas_engaged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment IN ('05250_projection_pr', '05250_declined_pr')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

SELECT '--- the limit still applies to the read that executes ---';
-- sum() cannot be answered from _minmax_count_projection, so the base table is read and must throw.
SELECT sum(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1 FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT sum(i) FROM x SETTINGS max_rows_to_read_leaf = 2, optimize_use_implicit_projections = 1 FORMAT Null; -- { serverError TOO_MANY_ROWS }
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 0 FORMAT Null; -- { serverError TOO_MANY_ROWS }

SELECT '--- unchanged without parallel replicas ---';
SELECT max(i) FROM x SETTINGS max_rows_to_read = 2, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0;

DROP TABLE x;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number FROM numbers(100);

SELECT '--- read order bounds the rows the query needs ---';
SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0, use_skip_indexes_for_top_k = 1, optimize_read_in_order = 1, max_threads = 1;
SELECT a FROM t ORDER BY a LIMIT 5 SETTINGS max_rows_to_read = 12, log_comment = '05250_read_order_pr';
SELECT a FROM t ORDER BY a LIMIT 20 FORMAT Null SETTINGS max_rows_to_read = 12; -- { serverError TOO_MANY_ROWS }

-- Same witness for the read-order shape, which reaches the estimate through a different plan.
SYSTEM FLUSH LOGS query_log;
SELECT argMax(ProfileEvents['ParallelReplicasQueryCount'], event_time_microseconds) > 0 AS parallel_replicas_engaged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND initial_query_id = query_id
  AND log_comment = '05250_read_order_pr'
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t;
