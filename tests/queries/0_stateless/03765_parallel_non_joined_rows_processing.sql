-- Tags: no-random-settings
SET explain_query_plan_default = 'legacy';

SET query_plan_join_swap_table = 0;

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

SELECT '--- Parallel: NULL keys and ON-filtered right rows ---';

SET max_threads = 8, parallel_non_joined_rows_processing = 1, max_block_size = 1000;
-- r: 100000 rows in 100 blocks, keys NULL when number % 7 = 0, ON drops right rows with v % 5 = 0

SET join_algorithm = 'hash';
SELECT count(), countIf(r.k IS NULL), countIf(l.n IS NULL), sum(r.v)
FROM (SELECT number AS n FROM numbers(1000)) AS l
RIGHT JOIN (SELECT if(number % 7 = 0, NULL, number) AS k, number AS v FROM numbers(100000)) AS r
ON l.n = r.k AND r.v % 5 != 0;
SELECT count(), countIf(r.k IS NULL), countIf(l.n IS NULL), sum(r.v)
FROM (SELECT number AS n FROM numbers(1000)) AS l
FULL JOIN (SELECT if(number % 7 = 0, NULL, number) AS k, number AS v FROM numbers(100000)) AS r
ON l.n = r.k AND r.v % 5 != 0;

SET join_algorithm = 'parallel_hash';
SELECT count(), countIf(r.k IS NULL), countIf(l.n IS NULL), sum(r.v)
FROM (SELECT number AS n FROM numbers(1000)) AS l
RIGHT JOIN (SELECT if(number % 7 = 0, NULL, number) AS k, number AS v FROM numbers(100000)) AS r
ON l.n = r.k AND r.v % 5 != 0;
SELECT count(), countIf(r.k IS NULL), countIf(l.n IS NULL), sum(r.v)
FROM (SELECT number AS n FROM numbers(1000)) AS l
FULL JOIN (SELECT if(number % 7 = 0, NULL, number) AS k, number AS v FROM numbers(100000)) AS r
ON l.n = r.k AND r.v % 5 != 0;
