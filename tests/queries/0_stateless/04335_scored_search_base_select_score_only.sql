-- Tags: no-fasttest

-- A query that reads only scorer-produced columns (`SELECT _score FROM
-- vectorSearch(...)`) must not build the lazy column-proxy branch: the lazy
-- reader cannot perform a zero-column read (`injectRequiredColumns` would
-- inject a physical column and the lazy chunks would not match the empty
-- lazy header).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- Two parts to exercise the merge of several scorer sources.
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [2.0, 0.0]);
INSERT INTO tab VALUES (2, [3.0, 0.0]), (3, [4.0, 0.0]);

SELECT '-- Only _score is selected';
SELECT round(_score, 2)
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3)
ORDER BY _score;

SELECT '-- The plan has no lazy branch';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%')
FROM (EXPLAIN SELECT _score FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3));

SELECT '-- Sanity check: selecting a source column still uses the lazy branch';
SELECT countIf(explain LIKE '%LazilyReadFromMergeTree%')
FROM (EXPLAIN SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3));

DROP TABLE tab;
