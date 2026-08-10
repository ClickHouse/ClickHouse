-- Tags: no-fasttest

-- `_part` is part metadata, so a predicate on it prunes the part list and must not also become a
-- bitmap prefilter: a prefilter reads rows only to mark them in a bitmap, spends the
-- `search_topk_prefilter_max_rows` budget and can hit the read limits before the index is scored.
-- For a mixed predicate the bitmap is built from the residual row-level part only, and over the
-- parts that survived the pruning.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

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

-- Insert into separate parts.
INSERT INTO tab VALUES (0, 1, [1.0, 0.0]), (1, 2, [1.1, 0.0]);
INSERT INTO tab VALUES (2, 1, [0.0, 1.0]), (3, 2, [0.0, 1.1]);

SELECT '-- a _part predicate alone does not gate the scorer behind a bitmap subquery';
SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4) WHERE _part = 'all_1_1_0')
WHERE explain LIKE '%DelayedPorts%';

SELECT '-- the rows of the requested part';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_1_1_0'
ORDER BY _score, id
SETTINGS log_comment = '04813_part_only';

SELECT '-- the same, with the prefilter row budget exhausted: a _part predicate does not need it';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_1_1_0'
ORDER BY _score, id
SETTINGS search_topk_prefilter_max_rows = 0;

SELECT '-- a mixed predicate still builds a bitmap, but only over the surviving part';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_1_1_0' AND payload = 2
ORDER BY _score, id
SETTINGS log_comment = '04813_part_and_payload';

SYSTEM FLUSH LOGS query_log;

SELECT '-- rows put into the bitmaps: none for the _part-only query, one row of the surviving part for the mixed one';
SELECT log_comment, ProfileEvents['ScoredSearchPrefilterBitmapRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment IN ('04813_part_only', '04813_part_and_payload')
ORDER BY log_comment;

DROP TABLE tab;
