SET max_threads = 1;
SET max_block_size = 16384;
SET prefer_external_sort_block_bytes = 0;
SET temporary_files_buffer_size = 1048576;
SET max_bytes_before_external_sort = 0;
SET max_bytes_ratio_before_external_sort = 0;
SET group_by_two_level_threshold = 1;
SET optimize_aggregation_in_order = 0;

-- Single-threaded input and fixed blocks keep spill boundaries deterministic.
SELECT 'sort', extract(explain, 'Spill: spilled .*')
FROM (EXPLAIN ANALYZE SELECT number FROM numbers_mt(262144) ORDER BY number DESC
    SETTINGS max_bytes_before_external_sort = 1)
WHERE explain LIKE '%Spill: spilled %';

-- Aggregation detaches its temporary files for the merge. The total survives file cleanup.
SELECT 'group_by', extract(explain, 'Spill: spilled .*')
FROM (EXPLAIN ANALYZE SELECT number, count() FROM numbers_mt(262144) GROUP BY number
    SETTINGS max_bytes_before_external_group_by = 1, enable_adaptive_aggregator = 0)
WHERE explain LIKE '%Spill: spilled %';

-- The order-restoration sort shares the scope with `DISTINCT`, including its spill writes.
SELECT 'distinct', extract(explain, 'Spill: spilled .*')
FROM (EXPLAIN ANALYZE SELECT DISTINCT number AS n FROM numbers_mt(131072) ORDER BY n + 1 DESC
    SETTINGS max_bytes_before_external_distinct = 1, optimize_distinct_in_order = 0)
WHERE explain LIKE '%Spill: spilled %';
