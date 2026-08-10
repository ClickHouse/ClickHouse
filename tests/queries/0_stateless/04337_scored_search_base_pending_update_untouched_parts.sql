-- Tags: no-fasttest

-- A part with a pending on-the-fly update of the indexed column cannot be
-- scored from the persisted index data. But when the prefilter excludes all
-- rows of such a part, the search result is unaffected and the query must
-- succeed instead of throwing `NOT_IMPLEMENTED`.

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
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (0, 1, [1.0, 0.0]), (1, 1, [1.1, 0.0]);

-- A lightweight update of the indexed column, applied on the fly to the first part.
UPDATE tab SET vec = [9.0, 9.0] WHERE payload = 1 SETTINGS allow_experimental_lightweight_update = 1;

-- The second part is created after the update and is not affected by it.
INSERT INTO tab VALUES (2, 2, [0.0, 1.0]), (3, 2, [0.0, 1.1]);

SELECT '-- The prefilter excludes all rows of the updated part: the query succeeds';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 1.0], 2)
WHERE payload = 2
ORDER BY id;

SELECT '-- The updated part contributes rows: the query still throws';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 1.0], 2)
WHERE payload = 1
ORDER BY id; -- { serverError NOT_IMPLEMENTED }

SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 1.0], 2)
ORDER BY id; -- { serverError NOT_IMPLEMENTED }

DROP TABLE tab;
