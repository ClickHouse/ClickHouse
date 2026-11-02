-- `query_plan_aggregation_bucket_top_k` controls both conversion-stage selection and threshold
-- merging. The global query-plan optimization switch disables both paths as well.
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;
SET query_plan_aggregation_bucket_top_k = 1;
SET enable_adaptive_aggregator = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET serialize_query_plan = 0;
SET max_rows_to_group_by = 0;
SET exact_rows_before_limit = 0;
SET max_threads = 4;
SET max_streams_to_max_threads_ratio = 0.25;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
-- A single scan stream requires external aggregation to be enabled to create a two-level table.
-- The threshold is above the dataset's size, so no data is spilled.
SET max_bytes_before_external_group_by = 10737418240;
SET max_bytes_ratio_before_external_group_by = 0;
SET log_queries = 1;
SET log_queries_probability = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET log_queries_min_query_duration_ms = 0;
SET log_profile_events = 1;

DROP TABLE IF EXISTS bucket_top_k_setting;
CREATE TABLE bucket_top_k_setting (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO bucket_top_k_setting SELECT number % 10000, number FROM numbers(100000);

-- A lone `count` uses conversion-stage selection, while an integer `max` uses threshold merging.
SELECT k, count() AS c FROM bucket_top_k_setting GROUP BY k ORDER BY c DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_a_count_enabled' FORMAT Null;
SELECT k, max(v) AS m FROM bucket_top_k_setting GROUP BY k ORDER BY m DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_b_max_enabled' FORMAT Null;

SELECT k, count() AS c FROM bucket_top_k_setting GROUP BY k ORDER BY c DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_c_count_disabled', query_plan_aggregation_bucket_top_k = 0 FORMAT Null;
SELECT k, max(v) AS m FROM bucket_top_k_setting GROUP BY k ORDER BY m DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_d_max_disabled', query_plan_aggregation_bucket_top_k = 0 FORMAT Null;

SELECT k, count() AS c FROM bucket_top_k_setting GROUP BY k ORDER BY c DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_e_count_global_disabled', query_plan_enable_optimizations = 0 FORMAT Null;
SELECT k, max(v) AS m FROM bucket_top_k_setting GROUP BY k ORDER BY m DESC LIMIT 3
    SETTINGS log_comment = '05212_abtk_f_max_global_disabled', query_plan_enable_optimizations = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05212_abtk_', ''),
    ProfileEvents['AggregationBucketTopKConversions'] > 0,
    ProfileEvents['AggregationThresholdTopKMerges'] > 0,
    ProfileEvents['AggregationThresholdTopKPrunedCells'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05212\_abtk\_%'
ORDER BY log_comment;

DROP TABLE bucket_top_k_setting;
