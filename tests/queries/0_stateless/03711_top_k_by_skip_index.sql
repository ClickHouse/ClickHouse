-- Test for verifying TopN optimizations
-- Tags: no-parallel-replicas

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0; -- for stable max_rows_to_read
SET read_overflow_mode = 'break';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    v1 Int32,
    v2 Int32,
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab SELECT number, number, number FROM numbers(10000);

-- Only 10 granules should be read
SELECT id, v1 FROM tab ORDER BY v1 ASC LIMIT 10 SETTINGS max_rows_to_read = 640, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0;
SELECT id, v1 FROM tab ORDER BY v1 DESC LIMIT 10 SETTINGS max_rows_to_read = 640, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0;

-- Verify EXPLAIN indexes=1 output to confirm skip index usage for top-n
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id, v1
    FROM tab
    ORDER BY v1 ASC
    LIMIT 10
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

--  If WHERE clause is present, TopN via skip index only optimization not possible - row should not seen
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT id, v1
    FROM tab
    WHERE v2 > 0
    ORDER BY v1 ASC
    LIMIT 10
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

-- Verify that dynamic filter injects PREWHERE dynamic filter
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN actions = 1
    SELECT id, v1
    FROM tab
    WHERE v2 > 0
    ORDER BY v1 ASC
    LIMIT 10
    SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1)
WHERE explain LIKE '%topK%';

-- Verify execution of dynamic filter
SELECT id, v1
FROM tab
WHERE v2 > 0
ORDER BY v1 ASC
LIMIT 5
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
