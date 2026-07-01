-- Tags: no-fasttest

-- `VectorScorer::validate` runs once per query before any part is scored,
-- so an invalid reference vector or search setting is rejected even when
-- the WHERE clause (and hence the prefilter bitmaps) filters out every row
-- of every part, or when the table has no parts at all. Without the
-- query-level hook the per-part validation in `scorePart` is skipped on
-- the empty-prefilter fast path and the invalid query silently succeeds
-- with an empty result.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id Int32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [0.0, 1.0]);

-- `id = -1` matches no row, so every per-part prefilter bitmap is empty
-- and no part reaches `scorePart`.

SELECT '-- all rows filtered out: dimension mismatch still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0, 0.0], 2) WHERE id = -1; -- { serverError INCORRECT_QUERY }

SELECT '-- all rows filtered out: NaN element still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, nan], 2) WHERE id = -1; -- { serverError INCORRECT_QUERY }

SELECT '-- all rows filtered out: Inf element still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, inf], 2) WHERE id = -1; -- { serverError INCORRECT_QUERY }

SELECT '-- all rows filtered out: zero candidate list size still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2) WHERE id = -1
SETTINGS hnsw_candidate_list_size_for_search = 0; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- all rows filtered out: valid query succeeds with empty result';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2) WHERE id = -1;

DROP TABLE tab;

-- The same checks must fire when the part set itself is empty.

CREATE TABLE tab (id Int32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

SELECT '-- empty table: dimension mismatch still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0, 0.0], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- empty table: NaN element still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, nan], 2); -- { serverError INCORRECT_QUERY }

SELECT '-- empty table: zero candidate list size still rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2)
SETTINGS hnsw_candidate_list_size_for_search = 0; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- empty table: valid query succeeds with empty result';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 2);

DROP TABLE tab;
