-- Tags: no-fasttest

-- The `WHERE` predicate of a scored search must be evaluated by the bitmap
-- prefilter, i.e. *before* the top-K scorer, so that the scorer returns the K
-- nearest rows among the rows that pass the filter. If the predicate were left
-- to run only as an outer filter on the global top-K, the result could be wrong
-- (here: empty), because the globally-nearest rows are exactly the ones the
-- filter removes.
--
-- With the analyzer, pushed-down filter nodes reference column identifiers (e.g.
-- `__table1.category`). `ReadFromMergeTreeScoredSearch::applyFilters` must remap
-- them back to source-table column names via `buildNodeNameToInputNodeColumn`,
-- otherwise the predicate would not enter the prefilter split. This test is
-- therefore pinned to `enable_analyzer = 1`.

SET allow_experimental_search_topk_table_functions = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    category String,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- ids 0-2 are the nearest to the search vector [1, 0] but have category 'a';
-- ids 3-5 are far from it but have category 'b'. With a top-3 search and
-- WHERE category = 'b' the result is the three 'b' rows only if the predicate
-- is applied before the scorer; a post-filter on the global top-3 (0, 1, 2)
-- would return nothing.
INSERT INTO tab VALUES
    (0, 'a', [1.0, 0.0]),
    (1, 'a', [1.1, 0.0]),
    (2, 'a', [0.9, 0.0]),
    (3, 'b', [0.0, 1.0]),
    (4, 'b', [0.0, 1.1]),
    (5, 'b', [0.0, 0.9]);

SELECT '-- prefilter applied before scorer: returns the three category = b rows';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 'b'
ORDER BY id
SETTINGS log_comment = '04357_scored_search_base_prefilter_before_scorer_analyzer';

SELECT '-- brute-force reference (rows that pass the filter, nearest first)';
SELECT id
FROM tab
WHERE category = 'b'
ORDER BY L2Distance(vec, [1.0, 0.0]), id
LIMIT 3;

SYSTEM FLUSH LOGS query_log;

SELECT '-- the predicate entered the prefilter: ScoredSearchPrefilterBitmapRows > 0';
SELECT ProfileEvents['ScoredSearchPrefilterBitmapRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04357_scored_search_base_prefilter_before_scorer_analyzer';

DROP TABLE tab;
