-- A RIGHT join built with the parallel layout emits its unmatched right rows on all `max_threads`
-- streams, through `NonJoinedBlocksTransform`, also for a `UInt8` key, whose parallel map is a
-- fixed-size map split into buckets.
-- Random settings limits: max_rows_in_join=(0, 0); max_bytes_in_join=(0, 0); max_bytes_before_external_join=(0, 0); max_bytes_ratio_before_external_join=(0, 0); max_threads=(4, 4); parallel_hash_join_threshold=(1, 1); parallel_non_joined_rows_processing=(1, 1)

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET parallel_hash_join_threshold = 1;
SET max_threads = 4;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET join_use_nulls = 1;
SET parallel_non_joined_rows_processing = 1;
SET enable_analyzer = 1;

SELECT 'key8_pipeline';
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toUInt8(number) AS key FROM numbers(128)) AS t1
    RIGHT JOIN (SELECT toUInt8(number) AS key FROM numbers(256)) AS t2
    ON t1.key = t2.key
    SETTINGS join_algorithm = 'hash'
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT 'key8_right';
SELECT count(), countIf(t1.key IS NOT NULL), countIf(t2.key IS NOT NULL)
FROM (SELECT toUInt8(number) AS key FROM numbers(128)) AS t1
RIGHT JOIN (SELECT toUInt8(number) AS key FROM numbers(256)) AS t2
ON t1.key = t2.key
SETTINGS join_algorithm = 'hash';
