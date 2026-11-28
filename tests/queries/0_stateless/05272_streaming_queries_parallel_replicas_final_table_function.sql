-- FINAL on a table function disables parallel replicas the same way as FINAL on a table: the query is refused
-- when parallel replicas are forced, and runs without them otherwise.

SET allow_experimental_time_series_table = 1;
SET enable_streaming_queries = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m1', {'job': 'j1'}, [(1, 1.)]), ('m2', {'job': 'j2'}, [(1, 2.)]);

SELECT metric_name FROM timeSeriesTags(currentDatabase(), 'ts') FINAL ORDER BY metric_name
SETTINGS enable_parallel_replicas = 1, parallel_replicas_local_plan = 0;
SELECT metric_name FROM timeSeriesTags(currentDatabase(), 'ts') FINAL ORDER BY metric_name
SETTINGS enable_parallel_replicas = 1, parallel_replicas_local_plan = 1;
SELECT metric_name FROM timeSeriesTags(currentDatabase(), 'ts') FINAL SETTINGS enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
-- STREAM on a table function is refused the same way.
SELECT metric_name FROM timeSeriesTags(currentDatabase(), 'ts') STREAM LIMIT 2 SETTINGS enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
-- Without FINAL or STREAM the same read is not refused; only the absence of the error is checked.
SELECT metric_name FROM timeSeriesTags(currentDatabase(), 'ts') SETTINGS enable_parallel_replicas = 2 FORMAT Null;

DROP TABLE IF EXISTS t_rmt;
CREATE TABLE t_rmt (k UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO t_rmt VALUES (1), (2);
SELECT k FROM merge(currentDatabase(), '^t_rmt$') FINAL SETTINGS enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_rmt;
DROP TABLE ts;
