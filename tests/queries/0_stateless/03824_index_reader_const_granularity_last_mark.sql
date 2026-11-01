-- Test that MergeTreeReaderIndex correctly handles the last granule
-- when use_const_adaptive_granularity=1 and index_granularity_bytes=0
-- (the last mark in the granularity may report more rows than actually exist).
-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    v1 Int32,
    v2 Int32,
    INDEX v1idx v1 TYPE minmax
) Engine = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    use_const_adaptive_granularity = 1,
    index_granularity_bytes = 0;

-- 10000 rows: 156 full granules of 64 + 1 partial granule of 16 rows
INSERT INTO tab SELECT number, number, number FROM numbers(10000);

-- This query triggers lazy materialization with index reader as the first reader in the chain.
-- The bug was that the index reader reported 64 rows for the last granule (instead of 16),
-- causing a mismatch with the actual data reader.
SELECT id, v1 FROM tab ORDER BY v1 DESC LIMIT 257
SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 0;

DROP TABLE tab;
