-- `GROUP BY` a nullable number. The first block creates the NULL group.
-- Then the table converts to two-level, and the NULL group must survive the conversion.
-- Without an aggregate function, the method is `nullable_key64_void` or `nullable_key32_void`.
-- With `count`, it is `nullable_key64` or `nullable_key32`.
SET max_threads = 1, max_block_size = 100, group_by_two_level_threshold = 1,
    max_bytes_before_external_group_by = 10000000000, max_bytes_ratio_before_external_group_by = 0,
    enable_group_by_top_k_optimization = 0, query_plan_aggregation_bucket_top_k = 0, log_queries = 1;

SELECT k FROM (SELECT if(number % 10 = 0, NULL, number) AS k FROM numbers(1000)) GROUP BY k ORDER BY k NULLS FIRST LIMIT 3;
SELECT count(), countIf(k IS NULL) FROM (SELECT k FROM (SELECT if(number % 10 = 0, NULL, number) AS k FROM numbers(1000)) GROUP BY k);
SELECT count(), countIf(k IS NULL), sumIf(c, k IS NULL) FROM (SELECT k, count() AS c FROM (SELECT if(number % 10 = 0, NULL, number) AS k FROM numbers(1000)) GROUP BY k);

SELECT k FROM (SELECT if(number % 10 = 0, NULL, toUInt32(number)) AS k FROM numbers(1000)) GROUP BY k ORDER BY k NULLS FIRST LIMIT 3;
SELECT count(), countIf(k IS NULL) FROM (SELECT k FROM (SELECT if(number % 10 = 0, NULL, toUInt32(number)) AS k FROM numbers(1000)) GROUP BY k);
SELECT count(), countIf(k IS NULL), sumIf(c, k IS NULL) FROM (SELECT k, count() AS c FROM (SELECT if(number % 10 = 0, NULL, toUInt32(number)) AS k FROM numbers(1000)) GROUP BY k);

-- Every query above must have gone through the conversion to two-level.
SYSTEM FLUSH LOGS query_log;
SELECT count(), countIf(ProfileEvents['AggregationConvertedToTwoLevel'] > 0) FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%numbers(1000)%' AND query NOT LIKE '%query_log%';
