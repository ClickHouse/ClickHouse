-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database, long

SET enable_json_type = 1;

DROP TABLE IF EXISTS force_shared_cache_bound_04839;

-- ColumnObject::shouldForceSharedData memoizes every distinct path matched by a SHARED REGEXP rule
-- in force_shared_data_paths, unbounded -- for a broad rule over a high-cardinality document where
-- most matched paths are seen exactly once, that cache grows without limit. The cache is now capped
-- at MAX_SHARED_DATA_STATISTICS_SIZE (10000); this is a pure memory fix, so the regression to guard
-- is that paths matched *after* the cache is full must still be correctly forced into shared data
-- (falling back to a direct regex re-evaluation) rather than accidentally treated as unmatched.
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
