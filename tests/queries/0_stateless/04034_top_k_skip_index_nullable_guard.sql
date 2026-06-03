-- Verify skip-index top-k is correctly guarded for nullable/collation types
-- while dynamic filtering still works for them.
-- Tags: no-parallel-replicas, no-fasttest

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;

-- Nullable column with minmax index: skip-index top-k should NOT activate,
-- but dynamic filtering should still produce correct results.
DROP TABLE IF EXISTS tab_nullable_idx;
CREATE TABLE tab_nullable_idx
(
    id UInt32,
    v1 Nullable(UInt32),
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_nullable_idx SELECT number, if(number % 100 = 0, NULL, number + 1) from numbers(10000);

SELECT 'EXPLAIN skip-index Nullable(UInt32): should be empty';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT v1
    FROM tab_nullable_idx
    ORDER BY v1 ASC
    LIMIT 10
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

SELECT 'Nullable(UInt32) ASC dynamic filter';
SELECT v1 FROM tab_nullable_idx ORDER BY v1 ASC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) ASC NULLS FIRST dynamic filter';
SELECT v1 FROM tab_nullable_idx ORDER BY v1 ASC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) DESC dynamic filter';
SELECT v1 FROM tab_nullable_idx ORDER BY v1 DESC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) DESC NULLS FIRST dynamic filter';
SELECT v1 FROM tab_nullable_idx ORDER BY v1 DESC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_nullable_idx;

-- String column with COLLATE and minmax index: skip-index top-k should NOT activate
DROP TABLE IF EXISTS tab_collate_idx;
CREATE TABLE tab_collate_idx
(
    id UInt32,
    v1 String,
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_collate_idx VALUES (1, 'banana'), (2, 'Apple'), (3, 'cherry'), (4, 'Date'), (5, 'elderberry'), (6, 'Fig'), (7, 'grape'), (8, 'Honeydew'), (9, 'icaco'), (10, 'Jackfruit');

SELECT 'EXPLAIN skip-index String COLLATE: should be empty';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT v1
    FROM tab_collate_idx
    ORDER BY v1 ASC COLLATE 'en'
    LIMIT 5
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

SELECT 'String ASC COLLATE en dynamic filter';
SELECT v1 FROM tab_collate_idx ORDER BY v1 ASC COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'String DESC COLLATE en dynamic filter';
SELECT v1 FROM tab_collate_idx ORDER BY v1 DESC COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_collate_idx;

-- Nullable(String) with COLLATE and minmax index: skip-index top-k should NOT activate
DROP TABLE IF EXISTS tab_nullable_string_collate_idx;
CREATE TABLE tab_nullable_string_collate_idx
(
    id UInt32,
    v1 Nullable(String),
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_nullable_string_collate_idx VALUES (1, 'banana'), (2, 'Apple'), (3, NULL), (4, 'Date'), (5, 'cherry'), (6, NULL), (7, 'Fig'), (8, 'grape');

SELECT 'EXPLAIN skip-index Nullable(String) COLLATE: should be empty';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT v1
    FROM tab_nullable_string_collate_idx
    ORDER BY v1 ASC COLLATE 'en'
    LIMIT 5
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

SELECT 'Nullable(String) ASC COLLATE en dynamic filter';
SELECT v1 FROM tab_nullable_string_collate_idx ORDER BY v1 ASC COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(String) ASC NULLS FIRST COLLATE en dynamic filter';
SELECT v1 FROM tab_nullable_string_collate_idx ORDER BY v1 ASC NULLS FIRST COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_nullable_string_collate_idx;

-- Non-nullable numeric column with minmax index: skip-index top-k SHOULD still work
DROP TABLE IF EXISTS tab_numeric_idx;
CREATE TABLE tab_numeric_idx
(
    id UInt32,
    v1 UInt32,
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_numeric_idx SELECT number, number + 1 FROM numbers(10000);

SELECT 'EXPLAIN skip-index UInt32: should show TopK';
SELECT trimLeft(explain) AS explain FROM (
    EXPLAIN indexes = 1
    SELECT v1
    FROM tab_numeric_idx
    ORDER BY v1 ASC
    LIMIT 10
    SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0)
WHERE explain LIKE '%TopK%';

DROP TABLE tab_numeric_idx;
