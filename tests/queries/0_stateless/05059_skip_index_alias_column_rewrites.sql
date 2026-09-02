-- Tags: no-parallel-replicas
-- Test that a skip index declared directly on an `ALIAS` column is used when the analyzer
-- rewrites the filter expression. The index expression stores the expanded form of the alias
-- (`multiIf(...)`), while the filter is rewritten by `optimize_multiif_to_if` to `if(...)`, so
-- without the rewrite-aware matching the index is not used at all.
-- Regression test for issue #103128.
SET explain_query_plan_default = 'legacy';

-- Disable statistics-based part pruning so that randomly injected `auto_statistics_types`
-- in CI does not add a Statistics section to the `EXPLAIN` output.
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS test_skip_idx_alias;

CREATE TABLE test_skip_idx_alias
(
    v Int32,
    s Int32 ALIAS multiIf(v > 50, v, 0),
    INDEX idx s TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO test_skip_idx_alias SELECT number FROM numbers(100);

-- Count the granules the index leaves, rather than matching the `EXPLAIN` output verbatim: the
-- number of granules per index is randomized by `merge_tree_coarse_index_granularity` in CI.
SELECT 'analyzer', countIf(explain LIKE '%Name: idx%') AS index_used, countIf(explain LIKE '%Granules: 1/25%') AS granules_left
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_alias WHERE s > 97)
SETTINGS enable_analyzer = 1;

SET enable_analyzer = 0;

SELECT 'legacy analyzer', countIf(explain LIKE '%Name: idx%') AS index_used, countIf(explain LIKE '%Granules: 1/25%') AS granules_left
FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_alias WHERE s > 97);

SET enable_analyzer = 1;

DROP TABLE test_skip_idx_alias;
