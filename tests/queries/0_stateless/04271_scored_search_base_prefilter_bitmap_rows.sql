-- Tags: no-fasttest

-- Plan 01 §"Tests": a non-index `WHERE` predicate goes through the
-- bitmap subquery; verify `ProfileEvents.ScoredSearchPrefilterBitmapRows
-- > 0` (rows kept in the prefilter bitmaps) and that the result matches
-- a brute-force `L2Distance` reference (asserting the prefilter is
-- honoured by the scorer).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

-- `payload` is not part of the PK and has no skip index — a filter on
-- it cannot be answered by index analysis and must be evaluated by the
-- bitmap subquery.
CREATE TABLE tab
(
    id Int32,
    payload UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES
    (0, 1, [1.0, 0.0]),
    (1, 2, [1.1, 0.0]),
    (2, 1, [0.0, 1.0]),
    (3, 2, [0.0, 1.1]),
    (4, 1, [0.5, 0.5]),
    (5, 2, [0.6, 0.6]);

SELECT '-- ids returned: only payload = 1';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 10)
WHERE payload = 1
ORDER BY id
SETTINGS log_comment = '04271_scored_search_base_prefilter_bitmap_rows';

SELECT '-- brute force reference for payload = 1';
SELECT id
FROM tab
WHERE payload = 1
ORDER BY L2Distance(vec, [1.0, 0.0]), id
LIMIT 10;

SYSTEM FLUSH LOGS query_log;

SELECT '-- ScoredSearchPrefilterBitmapRows > 0';
SELECT ProfileEvents['ScoredSearchPrefilterBitmapRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04271_scored_search_base_prefilter_bitmap_rows';

DROP TABLE tab;
