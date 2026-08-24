-- Asserts that the top-K threshold merge engages (and prunes) exactly for the query shapes it
-- serves, via its profile events. The plan shape and the merge path are pinned: no parallel
-- replicas (the dataflow-statistics updater forces the ordinary merge on purpose), no plan
-- serialization (the parameter deliberately does not survive it), forced two-level states and
-- several threads (the threshold merge serves the two-level final merge).

SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET serialize_query_plan = 0;
SET enable_aggregation_top_k_threshold_merge = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS threshold_top_k_events;
CREATE TABLE threshold_top_k_events (k UInt64, v Float64, u UInt64) ENGINE = MergeTree ORDER BY ();
-- A long uniform tail: 50000 keys with 4 rows each ...
INSERT INTO threshold_top_k_events SELECT intDiv(number, 4), number, number % 4 FROM numbers(200000);
-- ... and 20 heavy keys with 1000 rows and 1000 distinct values each, so any descending top-10
-- is dominated by the heavy keys and the threshold cuts the tail off after them.
INSERT INTO threshold_top_k_events SELECT 500000 + number % 20, 1e9 + number, number FROM numbers(20000);

SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_a_count' FORMAT Null;
SELECT k, uniqExact(u) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_b_uniq_exact' FORMAT Null;
SELECT k, max(u) AS m FROM threshold_top_k_events GROUP BY k ORDER BY m DESC LIMIT 10
    SETTINGS log_comment = '05043_c_max' FORMAT Null;

-- Shapes the threshold merge must not serve.
-- The estimating uniq: its estimate is not exactly subadditive.
SELECT k, uniq(u) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_d_uniq_estimate' FORMAT Null;
-- A floating-point ordering value: no NaN order is consistent with the merge in both directions.
SELECT k, max(v) AS m FROM threshold_top_k_events GROUP BY k ORDER BY m DESC LIMIT 10
    SETTINGS log_comment = '05043_e_float' FORMAT Null;
-- HAVING sits between the aggregation and the sort as a filter step.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k HAVING c > 1 ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_f_having' FORMAT Null;
-- More than one ORDER BY column: dropped boundary ties would break the tiebreaker.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC, k ASC LIMIT 10
    SETTINGS log_comment = '05043_g_two_columns' FORMAT Null;
-- The optimization is off.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_h_disabled', enable_aggregation_top_k_threshold_merge = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05043_', ''),
    ProfileEvents['AggregationThresholdTopKMerges'] > 0,
    ProfileEvents['AggregationThresholdTopKMergedGroups'] > 0,
    ProfileEvents['AggregationThresholdTopKPrunedCells'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05043\\_%'
ORDER BY log_comment;

DROP TABLE threshold_top_k_events;
