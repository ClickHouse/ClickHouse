-- Tags: no-random-settings

SET enable_analyzer = 1, query_plan_join_swap_table = 0;

SELECT '--- Correctness: FULL JOIN ---';

SET join_use_nulls = 1;

SET join_algorithm = 'hash';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
FULL JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key;

SET join_algorithm = 'parallel_hash';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
FULL JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key;

SELECT '--- Correctness: RIGHT JOIN ---';

SET join_algorithm = 'hash';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key;

SET join_algorithm = 'parallel_hash';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key;

SET join_algorithm = 'parallel_hash';
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
