-- Test for ORDER BY ... LIMIT n optimization (top-K) - dynamic PREWHERE filtering will be used to skip rows
-- Tags: long, no-tsan, no-asan, no-msan, no-s3-storage

DROP TABLE IF EXISTS tab1;

CREATE TABLE tab1
(
    id UInt32,
    v1 UInt32,
    v2 UInt32
) Engine = MergeTree ORDER BY id SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

-- INSERT in random id order, so that v1 is scattered, but v1 is essentially 1..1000000
INSERT INTO tab1 SELECT rand(), number + 1, number + 1 from numbers(1000000);

SELECT v1, v2 FROM tab1 ORDER BY v1 DESC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;
SELECT v1, v2 FROM tab1 ORDER BY v1 DESC LIMIT 20 SETTINGS use_top_k_dynamic_filtering=1;
SELECT v1, v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 DESC LIMIT 10 SETTINGS use_top_k_dynamic_filtering=1;

SELECT v1, v2 FROM tab1 ORDER BY v1 ASC LIMIT 5 SETTINGS use_top_k_dynamic_filtering=1;
SELECT v1, v2 FROM tab1 ORDER BY v1 ASC LIMIT 20 SETTINGS use_top_k_dynamic_filtering=1;
SELECT v1, v2 FROM tab1 WHERE v2 > 100000 ORDER BY v1 ASC LIMIT 10 SETTINGS use_top_k_dynamic_filtering=1;
