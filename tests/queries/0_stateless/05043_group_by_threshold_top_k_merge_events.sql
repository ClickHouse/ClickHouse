-- Asserts that the top-K threshold merge engages (and prunes) exactly for the query shapes it
-- serves, via its profile events. The plan shape and the merge path are pinned: no parallel
-- replicas (the dataflow-statistics updater forces the ordinary merge on purpose), no plan
-- serialization (the parameter deliberately does not survive it), forced two-level states over
-- a single deterministic scan stream (the threshold merge serves the two-level final merge,
-- which still runs on several threads, one bucket per worker).

SET max_threads = 4;
-- One scan stream, so the aggregation builds exactly one table and the merge layout does not
-- depend on how the storage backend splits the read into tasks (the s3/azure read pools split
-- by part, and a stream with few distinct keys would survive as a second table: its head then
-- keeps the summing threshold open on the tied tail values, the pop budget trips, and the
-- verdict sends every bucket to the ordinary merge - no pruning). The merge stage still runs
-- on `max_threads`, one bucket per worker.
SET max_streams_to_max_threads_ratio = 0.25;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
-- Single-stream aggregation honors `group_by_two_level_threshold` only when the external
-- aggregation is possible (its two-level table otherwise serves nothing but the spill), so pin
-- a spill threshold that is enabled but can never trigger on this dataset - the CI settings
-- randomization sets both external-group-by settings to 0 otherwise.
SET max_bytes_before_external_group_by = 10737418240;
SET max_bytes_ratio_before_external_group_by = 0;
-- The adaptive aggregator stands down for a single-stream scan anyway; pin it off so the shape
-- does not change if that rejection is ever lifted.
SET enable_adaptive_aggregator = 0;
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

-- Ordering by the lone count is served by the conversion-stage bucket selection (the last
-- output column below), which the threshold merge yields to.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_a_count_alone' FORMAT Null;
-- When other aggregates ride along, the threshold merge takes over instead: it also skips
-- merging the losers' other states, which the conversion-stage selection cannot.
SELECT k, count() AS c, max(u) FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_b_count_riders' FORMAT Null;
SELECT k, uniqExact(u) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_c_uniq_exact' FORMAT Null;
-- The extremum bound serves any number of per-thread tables.
SELECT k, max(u) AS m FROM threshold_top_k_events GROUP BY k ORDER BY m DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_d_max' FORMAT Null;
-- The sum of unsigned integers is subadditive just like the count.
SELECT k, sum(u) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_e_sum' FORMAT Null;
-- The `If` and `Null` combinators forward the subadditive bound of the nested function.
SELECT k, countIf(u >= 2) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_f_count_if' FORMAT Null;
SELECT k, uniqExact(nullIf(u, 0)) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_g_uniq_exact_null' FORMAT Null;

-- Shapes the threshold merge must not serve.
-- The estimating uniq: its estimate is not exactly subadditive.
SELECT k, uniq(u) AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_h_uniq_estimate' FORMAT Null;
-- A floating-point ordering value: no NaN order is consistent with the merge in both directions.
SELECT k, max(v) AS m FROM threshold_top_k_events GROUP BY k ORDER BY m DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_i_float' FORMAT Null;
-- A String ordering value: the up-front peek of every cell's partial value would copy the whole
-- ordering payload before anything is pruned.
SELECT k, max(toString(u)) AS m FROM threshold_top_k_events GROUP BY k ORDER BY m DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_j_string' FORMAT Null;
-- HAVING sits between the aggregation and the sort as a filter step.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k HAVING c > 1 ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_k_having' FORMAT Null;
-- More than one ORDER BY column: dropped boundary ties would break the tiebreaker.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC, k ASC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_l_two_columns' FORMAT Null;
-- The optimization is off.
SELECT k, count() AS c FROM threshold_top_k_events GROUP BY k ORDER BY c DESC LIMIT 10
    SETTINGS log_comment = '05043_ttkm_m_disabled', enable_aggregation_top_k_threshold_merge = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05043_ttkm_', ''),
    ProfileEvents['AggregationThresholdTopKMerges'] > 0,
    ProfileEvents['AggregationThresholdTopKMergedGroups'] > 0,
    ProfileEvents['AggregationThresholdTopKPrunedCells'] > 0,
    ProfileEvents['AggregationBucketTopKConversions'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05043\_ttkm\_%'
ORDER BY log_comment;

DROP TABLE threshold_top_k_events;
