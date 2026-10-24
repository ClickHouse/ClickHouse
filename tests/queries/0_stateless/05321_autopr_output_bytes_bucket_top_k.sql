-- Tags: no-parallel-replicas
-- no-parallel-replicas: the bucket Top-K conversion requires a final aggregation.

-- The output-bytes estimate of an aggregation must not depend on whether the final merge keeps only
-- each bucket's top rows (`query_plan_aggregation_bucket_top_k`).

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';
-- The conversion runs on a two-level table only. A single-stream read keeps the thresholds only with a
-- non-zero external threshold (never reached here), and an adaptive table goes two-level once frozen.
SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;
SET max_bytes_before_external_group_by = 10737418240, max_bytes_ratio_before_external_group_by = 0;
SET adaptive_aggregator_freeze_threshold = 0;

DROP TABLE IF EXISTS t_bucket_top_k;
CREATE TABLE t_bucket_top_k (k String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bucket_top_k SELECT concat('key-padding-to-thirty-bytes--', toString(number % 30000)) FROM numbers(100000);

SELECT k, count() AS c FROM t_bucket_top_k GROUP BY k ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS query_plan_aggregation_bucket_top_k = 1, log_comment = 'bucket_top_k_on';
SELECT k, count() AS c FROM t_bucket_top_k GROUP BY k ORDER BY c DESC LIMIT 10 FORMAT Null
    SETTINGS query_plan_aggregation_bucket_top_k = 0, log_comment = 'bucket_top_k_off';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
DROP TABLE t_bucket_top_k;
SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AggregationBucketTopKConversions'] > 0 AS top_k_applied,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS statistics_collected
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
  AND current_database = currentDatabase() AND log_comment IN ('bucket_top_k_on', 'bucket_top_k_off')
ORDER BY log_comment;

SELECT greatest(on_bytes, off_bytes) <= least(on_bytes, off_bytes) * 1.25 AS estimate_matches
FROM
(
    SELECT
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = 'bucket_top_k_on') AS on_bytes,
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = 'bucket_top_k_off') AS off_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment IN ('bucket_top_k_on', 'bucket_top_k_off')
);
