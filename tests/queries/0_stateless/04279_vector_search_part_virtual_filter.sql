-- Tags: no-fasttest

-- Per plan 02 §step 5: vectorSearch supports the `_part` virtual column
-- in the WHERE clause. `filterRangesByVirtualColumn` runs against the
-- ranges list before the scoring lambda, dropping unmatched parts but
-- preserving each survivor's `part_index_in_query` (so per-part bitmap
-- lookups still resolve correctly).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- Insert into separate parts.
INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);
INSERT INTO tab VALUES (2, [0.0, 1.0]), (3, [0.0, 1.1]);
INSERT INTO tab VALUES (4, [2.0, 0.0]), (5, [2.1, 0.0]);

SELECT '-- Top-K across all parts: 6 rows total, K=4';
SELECT id, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
ORDER BY _score, id;

SELECT '-- Filter to a single part by _part = all_1_1_0';
SELECT id, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_1_1_0'
ORDER BY _score, id;

SELECT '-- Filter to a single part by _part = all_2_2_0';
SELECT id, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_2_2_0'
ORDER BY _score, id;

SELECT '-- Filter to two parts via IN';
SELECT id, _part
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 6)
WHERE _part IN ('all_1_1_0', 'all_3_3_0')
ORDER BY _score, id;

SELECT '-- Filter to a non-existent part: 0 rows';
SELECT count()
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE _part = 'all_999_999_0';

DROP TABLE tab;
