-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database, long, no-tsan, no-asan, no-msan, no-ubsan

SET enable_json_type = 1;

DROP TABLE IF EXISTS force_shared_cache_bound_04839;

-- ColumnObject::shouldForceSharedData's match cache is now capped at MAX_SHARED_DATA_STATISTICS_SIZE
-- (10000); paths matched after it fills must still fall back to direct regex re-evaluation, not go unmatched.
CREATE TABLE force_shared_cache_bound_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10000, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO force_shared_cache_bound_04839
SELECT number AS id, ('{"tag_' || toString(number) || '":1}')::JSON(max_dynamic_paths=10000) AS j FROM numbers(15000);

-- No row (including ids well past the 10000-entry cache bound) should have a dynamic path: every
-- tag_N key matches the rule and must be forced into shared data regardless of cache state.
SELECT 'rows with a dynamic path (expect 0)', countIf(length(JSONDynamicPaths(j)) > 0) FROM force_shared_cache_bound_04839;

-- The value for a path well past the cache bound is still correctly readable via shared data.
SELECT 'value past cache bound', j.tag_14999 FROM force_shared_cache_bound_04839 WHERE id = 14999;

DROP TABLE force_shared_cache_bound_04839;
