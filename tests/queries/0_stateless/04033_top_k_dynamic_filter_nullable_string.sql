-- Test top-k dynamic filtering for Nullable, String, and collation types
-- Tags: no-fasttest

-- Nullable(UInt32) ORDER BY ... LIMIT with NULLS LAST (default)
DROP TABLE IF EXISTS tab_nullable;
CREATE TABLE tab_nullable
(
    id UInt32,
    v1 Nullable(UInt32)
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_nullable SELECT number, if(number % 100 = 0, NULL, number + 1) from numbers(10000);

SELECT 'Nullable(UInt32) ASC';
SELECT v1 FROM tab_nullable ORDER BY v1 ASC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) ASC NULLS FIRST';
SELECT v1 FROM tab_nullable ORDER BY v1 ASC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) DESC';
SELECT v1 FROM tab_nullable ORDER BY v1 DESC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(UInt32) DESC NULLS FIRST';
SELECT v1 FROM tab_nullable ORDER BY v1 DESC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_nullable;

-- String ORDER BY ... LIMIT
DROP TABLE IF EXISTS tab_string;
CREATE TABLE tab_string
(
    id UInt32,
    v1 String
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_string SELECT number, lpad(toString(number + 1), 6, '0') from numbers(10000);

SELECT 'String ASC';
SELECT v1 FROM tab_string ORDER BY v1 ASC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'String DESC';
SELECT v1 FROM tab_string ORDER BY v1 DESC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_string;

-- Nullable(String) ORDER BY ... LIMIT
DROP TABLE IF EXISTS tab_nullable_string;
CREATE TABLE tab_nullable_string
(
    id UInt32,
    v1 Nullable(String)
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_nullable_string SELECT number, if(number % 100 = 0, NULL, lpad(toString(number + 1), 6, '0')) from numbers(10000);

SELECT 'Nullable(String) ASC';
SELECT v1 FROM tab_nullable_string ORDER BY v1 ASC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(String) ASC NULLS FIRST';
SELECT v1 FROM tab_nullable_string ORDER BY v1 ASC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'Nullable(String) DESC';
SELECT v1 FROM tab_nullable_string ORDER BY v1 DESC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_nullable_string;

-- String with COLLATE
DROP TABLE IF EXISTS tab_collate;
CREATE TABLE tab_collate
(
    id UInt32,
    v1 String
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_collate VALUES (1, 'banana'), (2, 'Apple'), (3, 'cherry'), (4, 'Date'), (5, 'elderberry'), (6, 'Fig'), (7, 'grape'), (8, 'Honeydew'), (9, 'icaco'), (10, 'Jackfruit');

SELECT 'String ASC COLLATE en';
SELECT v1 FROM tab_collate ORDER BY v1 ASC COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

SELECT 'String DESC COLLATE en';
SELECT v1 FROM tab_collate ORDER BY v1 DESC COLLATE 'en' LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;

DROP TABLE tab_collate;
