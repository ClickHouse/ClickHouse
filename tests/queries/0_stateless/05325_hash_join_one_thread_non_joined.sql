-- With one build thread, unmatched rows of a RIGHT or FULL join stay on `JoiningTransform`,
-- even when the right side is above `parallel_hash_join_threshold`.
-- Random settings limits: max_threads=(1, 1)

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 1;
SET max_threads = 1;
SET parallel_non_joined_rows_processing = 1;
SET query_plan_join_swap_table = 0;
SET join_use_nulls = 1;

SELECT '--- NonJoinedBlocksTransform count ---';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
    FULL JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
    ON t1.key = t2.key
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT '--- RIGHT ---';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
RIGHT JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
ON t1.key = t2.key;

SELECT '--- FULL ---';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
FULL JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
ON t1.key = t2.key;
