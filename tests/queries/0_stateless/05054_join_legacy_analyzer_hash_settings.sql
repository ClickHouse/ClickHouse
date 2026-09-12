-- `TableJoin(Settings)` (`JoinedTables::makeTableJoin`, `enable_analyzer = 0`)
-- copies `joined_block_split_single_row` and `parallel_non_joined_rows_processing`.
-- Random settings limits: max_threads=(4, 4); parallel_hash_join_threshold=(1, 1); enable_analyzer=(0, 0)

SET enable_analyzer = 0;
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 1;
SET max_threads = 4;

SELECT '--- joined_block_split_single_row ---';
SELECT *
FROM system.one AS t1
JOIN system.one AS t2 ON t1.dummy = t2.dummy
SETTINGS enable_analyzer = 0, max_joined_block_size_rows = 0, joined_block_split_single_row = 1
FORMAT Null; -- { serverError NOT_IMPLEMENTED }

SELECT '--- parallel_non_joined_rows_processing = 1 ---';
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(200000)) AS t1
    FULL JOIN (SELECT toString(number + 100000) AS key FROM numbers(200000)) AS t2
    ON t1.key = t2.key
    SETTINGS enable_analyzer = 0, join_algorithm = 'hash', parallel_hash_join_threshold = 1,
             max_threads = 4, parallel_non_joined_rows_processing = 1,
             query_plan_join_swap_table = 0, enable_parallel_replicas = 0
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT '--- parallel_non_joined_rows_processing = 0 ---';
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(200000)) AS t1
    FULL JOIN (SELECT toString(number + 100000) AS key FROM numbers(200000)) AS t2
    ON t1.key = t2.key
    SETTINGS enable_analyzer = 0, join_algorithm = 'hash', parallel_hash_join_threshold = 1,
             max_threads = 4, parallel_non_joined_rows_processing = 0,
             query_plan_join_swap_table = 0, enable_parallel_replicas = 0
) WHERE explain LIKE '%NonJoinedBlocksTransform%';
