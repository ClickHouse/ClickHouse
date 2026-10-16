SET max_threads = 4;
SET max_block_size = 16384;
SET prefer_external_sort_block_bytes = 0;
SET temporary_files_buffer_size = 1048576;
SET group_by_two_level_threshold = 1;
SET optimize_aggregation_in_order = 0;

-- Shared scopes must be counted once, even when several processors write spill files.
-- The sorting threshold is divided between four streams; one byte per stream spills every block.
SELECT 'sort', extract(explain, 'Spill: spilled .*')
FROM (EXPLAIN ANALYZE SELECT number FROM numbers_mt(262144) ORDER BY number DESC
    SETTINGS max_bytes_before_external_sort = 4, max_bytes_ratio_before_external_sort = 0)
WHERE explain LIKE '%Spill: spilled %';

SELECT 'group_by', extract(explain, 'Spill: spilled .*')
FROM (EXPLAIN ANALYZE SELECT number, count() FROM numbers_mt(262144) GROUP BY number
    SETTINGS max_bytes_before_external_group_by = 1, enable_adaptive_aggregator = 0)
WHERE explain LIKE '%Spill: spilled %';
