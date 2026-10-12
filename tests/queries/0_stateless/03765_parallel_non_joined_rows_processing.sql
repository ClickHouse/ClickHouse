-- Tags: no-random-settings
SET explain_query_plan_default = 'legacy';

SET enable_analyzer = 1, query_plan_join_swap_table = 0;

SELECT '--- Correctness: FULL JOIN ---';

SET join_use_nulls = 1;

-- A subquery over `numbers` has no row estimate, and without one the threshold has no effect. The
-- `LIMIT` in the serial queries gives their right side an estimate below their threshold. Threshold 0
-- keeps the other queries parallel when the hash table statistics of an earlier run give an estimate.
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 0;

SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
FULL JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000) LIMIT 50000) AS t2
ON t1.key = t2.key
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '03765_full_serial';

SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
FULL JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key
SETTINGS log_comment = '03765_full_parallel';

SELECT '--- Correctness: RIGHT JOIN ---';

SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000) LIMIT 50000) AS t2
ON t1.key = t2.key
SETTINGS parallel_hash_join_threshold = 1000000000, log_comment = '03765_right_serial';

SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key
SETTINGS log_comment = '03765_right_parallel';

SET max_threads = 4;

SELECT '--- Parallel: NonJoinedBlocksTransform count ---';

SET parallel_non_joined_rows_processing = 1;
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(200000)) AS t1
    FULL JOIN (SELECT toString(number + 100000) AS key FROM numbers(200000)) AS t2
    ON t1.key = t2.key
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT '--- Sequential: NonJoinedBlocksTransform count ---';

SET parallel_non_joined_rows_processing = 0;
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(200000)) AS t1
    FULL JOIN (SELECT toString(number + 100000) AS key FROM numbers(200000)) AS t2
    ON t1.key = t2.key
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

-- Each serial query must have built the serial layout, and each parallel query the parallel one.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('03765_full_serial', '03765_full_parallel', '03765_right_serial', '03765_right_parallel')
ORDER BY event_time_microseconds;
