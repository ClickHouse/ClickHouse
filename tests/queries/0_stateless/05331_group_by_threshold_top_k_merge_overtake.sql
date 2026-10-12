-- The summing threshold merge over a dominant table and a small second one: a group seen later in
-- the walk (by its partial value in the dominant table) can overtake an earlier candidate once the
-- leftover from the other table is merged in, and must replace it in the candidate heap.

SET max_threads = 2;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 0;
SET max_rows_to_group_by = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET query_plan_aggregation_bucket_top_k = 1;
SET log_queries = 1;

-- Two scan streams, so the aggregation builds two tables.
-- The dominant one: a long tail of 100000 keys with one row each, and 2000 heavy keys - the odd
-- ones with 100 rows, the even ones with 95 rows.
-- The small one: 10 more rows for every even heavy key (105 in total), and 20 for the key 0 (115).
-- The walk over a bucket pops an odd key (100) before an even one (95), which then overtakes it.
CREATE VIEW threshold_top_k_overtake AS
    SELECT arrayJoin(if(number < 2000, arrayResize([number], if(number % 2 = 1, 100, 95), number), [number + 1000000])) AS k, materialize(1) AS u
    FROM numbers(102000)
    UNION ALL
    SELECT arrayJoin(arrayResize([number * 2], if(number = 0, 20, 10), number * 2)) AS k, materialize(1) AS u
    FROM numbers(1000);

SELECT k, count() AS c, max(u) FROM threshold_top_k_overtake GROUP BY k ORDER BY c DESC LIMIT 1
    SETTINGS log_comment = '05331_ttkm_count';
SELECT k, sum(u) AS s FROM threshold_top_k_overtake GROUP BY k ORDER BY s DESC LIMIT 1
    SETTINGS log_comment = '05331_ttkm_sum';

-- The same queries without the optimization.
SELECT k, count() AS c, max(u) FROM threshold_top_k_overtake GROUP BY k ORDER BY c DESC LIMIT 1
    SETTINGS query_plan_aggregation_bucket_top_k = 0;
SELECT k, sum(u) AS s FROM threshold_top_k_overtake GROUP BY k ORDER BY s DESC LIMIT 1
    SETTINGS query_plan_aggregation_bucket_top_k = 0;

SYSTEM FLUSH LOGS query_log;

-- The threshold merge has served both queries.
SELECT replaceOne(log_comment, '05331_ttkm_', ''), ProfileEvents['AggregationThresholdTopKMerges'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05331\_ttkm\_%'
ORDER BY log_comment;

DROP VIEW threshold_top_k_overtake;
