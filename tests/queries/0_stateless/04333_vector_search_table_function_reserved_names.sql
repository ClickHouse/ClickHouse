-- Tags: no-fasttest

-- `vectorSearch` appends `_score` / `_part` to the exposed columns and uses
-- `__global_row_index` as the internal row locator. The bitmap prefilter
-- subquery additionally reads the `_part_index` / `_part_offset` MergeTree
-- locator virtuals. A source table may legally declare columns with any of
-- these names; such schemas must be rejected with a clear error instead of
-- shadowing the virtual and mapping bitmap bits to the wrong rows.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab_resv_score;
DROP TABLE IF EXISTS tab_resv_part;
DROP TABLE IF EXISTS tab_resv_locator;
DROP TABLE IF EXISTS tab_resv_part_index;
DROP TABLE IF EXISTS tab_resv_part_offset;

CREATE TABLE tab_resv_score(id Int32, `_score` Float32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_resv_part(id Int32, `_part` String, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_resv_locator(id Int32, `__global_row_index` UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_resv_part_index(id Int32, `_part_index` UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE tab_resv_part_offset(id Int32, `_part_offset` UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2))
ENGINE = MergeTree ORDER BY id;

SELECT '-- a source column named _score is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_resv_score, idx, [0.0, 0.0], 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- a source column named _part is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_resv_part, idx, [0.0, 0.0], 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- a source column named __global_row_index is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_resv_locator, idx, [0.0, 0.0], 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- a source column named _part_index is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_resv_part_index, idx, [0.0, 0.0], 1); -- { serverError BAD_ARGUMENTS }

SELECT '-- a source column named _part_offset is rejected';
SELECT id FROM vectorSearch(currentDatabase(), tab_resv_part_offset, idx, [0.0, 0.0], 1); -- { serverError BAD_ARGUMENTS }

DROP TABLE tab_resv_score;
DROP TABLE tab_resv_part;
DROP TABLE tab_resv_locator;
DROP TABLE tab_resv_part_index;
DROP TABLE tab_resv_part_offset;
