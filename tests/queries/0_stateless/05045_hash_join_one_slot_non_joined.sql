-- `hash` with `parallel_hash_join_threshold` hit still builds a parallel map, but
-- `max_threads = 1` is one slot. Unmatched RIGHT/FULL rows must stay on the serial
-- JoiningTransform path: wiring NonJoinedBlocksTransform here has nothing to split.
-- Random settings limits: parallel_hash_join_threshold=(1, 1); max_threads=(1, 1)

SELECT '--- pipeline ---';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
    FULL JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
    ON t1.key = t2.key
    SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 1, max_threads = 1, parallel_non_joined_rows_processing = 1, query_plan_join_swap_table = 0, enable_analyzer = 1
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT '--- RIGHT ---';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
RIGHT JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
ON t1.key = t2.key
SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 1, max_threads = 1, parallel_non_joined_rows_processing = 1, query_plan_join_swap_table = 0, enable_analyzer = 1, join_use_nulls = 1;

SELECT '--- FULL ---';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(1000)) AS t1
FULL JOIN (SELECT toString(number + 500) AS key FROM numbers(1000)) AS t2
ON t1.key = t2.key
SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 1, max_threads = 1, parallel_non_joined_rows_processing = 1, query_plan_join_swap_table = 0, enable_analyzer = 1, join_use_nulls = 1;
