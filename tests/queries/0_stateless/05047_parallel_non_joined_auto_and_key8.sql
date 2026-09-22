-- `join_algorithm = 'auto'` stay-in-memory: `JoinSwitcher` must wire
-- `NonJoinedBlocksTransform` for unmatched RIGHT/FULL rows.
-- `key8` / `parallel_hash` uses one physical bucket; stream ownership is the routed bucket.
-- Random settings limits: max_rows_in_join=(0, 0); max_bytes_in_join=(0, 0); max_bytes_before_external_join=(0, 0); max_bytes_ratio_before_external_join=(0, 0); max_threads=(4, 4); parallel_hash_join_threshold=(1, 1); parallel_non_joined_rows_processing=(1, 1)

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'auto';
SET parallel_hash_join_threshold = 1;
SET max_threads = 4;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET join_use_nulls = 1;
SET parallel_non_joined_rows_processing = 1;
SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';

SELECT 'auto_pipeline';
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toString(number) AS key FROM numbers(200000)) AS t1
    FULL JOIN (SELECT toString(number + 100000) AS key FROM numbers(200000)) AS t2
    ON t1.key = t2.key
    SETTINGS max_threads = 4, query_plan_join_swap_table = 0, enable_analyzer = 1, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 0, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, parallel_non_joined_rows_processing = 1, join_use_nulls = 1
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT 'auto_right';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
RIGHT JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key
SETTINGS max_threads = 4, query_plan_join_swap_table = 0, enable_analyzer = 1, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 0, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, parallel_non_joined_rows_processing = 1, join_use_nulls = 1;

SELECT 'auto_full';
SELECT count(), countIf(t1.key != ''), countIf(t2.key != '')
FROM (SELECT toString(number) AS key FROM numbers(50000)) AS t1
FULL JOIN (SELECT toString(number + 25000) AS key FROM numbers(50000)) AS t2
ON t1.key = t2.key
SETTINGS max_threads = 4, query_plan_join_swap_table = 0, enable_analyzer = 1, join_algorithm = 'auto', parallel_hash_join_threshold = 1, max_rows_in_join = 0, max_bytes_in_join = 0, max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, parallel_non_joined_rows_processing = 1, join_use_nulls = 1;

SELECT 'key8_pipeline';
SELECT count(*)
FROM (
    EXPLAIN PIPELINE
    SELECT count()
    FROM (SELECT toUInt8(number) AS key FROM numbers(128)) AS t1
    RIGHT JOIN (SELECT toUInt8(number) AS key FROM numbers(256)) AS t2
    ON t1.key = t2.key
    SETTINGS max_threads = 4, query_plan_join_swap_table = 0, enable_analyzer = 1, join_algorithm = 'parallel_hash', parallel_non_joined_rows_processing = 1, join_use_nulls = 1
) WHERE explain LIKE '%NonJoinedBlocksTransform%';

SELECT 'key8_right';
SELECT count(), countIf(t1.key IS NOT NULL), countIf(t2.key IS NOT NULL)
FROM (SELECT toUInt8(number) AS key FROM numbers(128)) AS t1
RIGHT JOIN (SELECT toUInt8(number) AS key FROM numbers(256)) AS t2
ON t1.key = t2.key
SETTINGS max_threads = 4, query_plan_join_swap_table = 0, enable_analyzer = 1, join_algorithm = 'parallel_hash', parallel_non_joined_rows_processing = 1, join_use_nulls = 1;
