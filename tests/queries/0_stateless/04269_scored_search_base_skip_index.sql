-- Tags: no-fasttest

-- Plan 01 §"Tests": `StorageMergeTreeScoredSearchBase` composes with
-- regular skip indices. With a `minmax` skip index on `price` and
-- a `WHERE price < 10` filter, marks whose minmax slot proves
-- `price >= 10` must be pruned, leaving
-- `ProfileEvents.SelectedMarks < SelectedMarksTotal`.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

-- One mark per row; the minmax index then has one slot per row and
-- prunes individually. Granularity 1 on the skip index keeps the
-- mark→index alignment 1:1.
CREATE TABLE tab
(
    id Int32,
    price Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000,
    INDEX price_idx price TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;

SYSTEM STOP MERGES tab;

-- Two cheap rows (price < 10) and two pricey rows. The minmax index
-- must prune the two pricey marks.
INSERT INTO tab VALUES
    (0, 5,   [1.0, 0.0]),
    (1, 7,   [1.1, 0.0]),
    (2, 100, [1.0, 0.0]),
    (3, 200, [1.1, 0.0]);

SELECT '-- ids returned: only cheap rows';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 5)
WHERE price < 10
ORDER BY id
SETTINGS log_comment = '04269_scored_search_base_skip_index';

SYSTEM FLUSH LOGS query_log;

SELECT '-- SelectedMarks < SelectedMarksTotal';
SELECT ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04269_scored_search_base_skip_index';

DROP TABLE tab;
