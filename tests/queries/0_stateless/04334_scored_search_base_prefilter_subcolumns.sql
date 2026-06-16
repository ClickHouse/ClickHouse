-- Tags: no-fasttest

-- A `WHERE` predicate over subcolumns of source-table columns (`arr.size0`,
-- tuple elements, typed `JSON` paths) must be evaluated by the bitmap
-- prefilter, i.e. before the top-K scorer. Otherwise the scorer would pick
-- the K nearest rows among all rows and the filter above the search step
-- could drop all of them.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    arr Array(Int32),
    t Tuple(a UInt32, b String),
    json JSON(k UInt32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- ids 0-2 are the nearest to the search vector but do not pass the filters;
-- ids 3-5 are farther and pass them. With top-3 search the result is empty
-- unless the predicate is applied before the scorer.
INSERT INTO tab VALUES
    (0, [1.0, 0.0], [], (0, 'x'), '{"k" : 0}'),
    (1, [1.1, 0.0], [], (0, 'x'), '{"k" : 0}'),
    (2, [0.9, 0.0], [], (0, 'x'), '{"k" : 0}'),
    (3, [0.0, 1.0], [1], (1, 'y'), '{"k" : 1}'),
    (4, [0.0, 1.1], [1, 2], (1, 'y'), '{"k" : 1}'),
    (5, [0.0, 0.9], [1, 2, 3], (1, 'y'), '{"k" : 1}');

SELECT '-- Array size subcolumn predicate';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE arr.size0 > 0
ORDER BY id
SETTINGS log_comment = '04334_scored_search_base_prefilter_subcolumns';

SELECT '-- Tuple element predicate';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE t.a = 1
ORDER BY id;

SELECT '-- Typed JSON path predicate';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE json.k = 1
ORDER BY id;

SYSTEM FLUSH LOGS query_log;

SELECT '-- ScoredSearchPrefilterBitmapRows > 0';
SELECT ProfileEvents['ScoredSearchPrefilterBitmapRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04334_scored_search_base_prefilter_subcolumns';

DROP TABLE tab;
