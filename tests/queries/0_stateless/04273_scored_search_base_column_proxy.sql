-- Tags: no-fasttest

-- Plan 01d §"Tests": the `JoinLazyColumnsStep` wiring (column-proxy
-- stage) preserves four properties when consumed end-to-end via
-- `vectorSearch`:
--   1. Each output row carries the source columns read from the
--      correct part at the correct `_part_offset` (back-converted
--      from `__global_row_index`).
--   2. The `_part` virtual column is correctly emitted by the lazy
--      reader.
--   3. Empty main input produces no output rows.
--   4. The combined header drops `__global_row_index`.
--
-- Lands as a stateless end-to-end test (Option B of W2.3c manifest;
-- `gtest_merge_tree_scored_search_column_proxy` is deferred — see
-- final report).

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

-- Two parts so the lazy reader has to route `__global_row_index` to
-- the correct part by index, not by accident.
CREATE TABLE tab
(
    id Int32,
    s String,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (0, 'zero', [1.0, 0.0]), (1, 'one', [1.1, 0.0]);
INSERT INTO tab VALUES (2, 'two', [0.0, 1.0]), (3, 'three', [0.0, 1.1]);

SELECT '-- 1. Source columns join to the correct row';
-- `id` and `s` are lazy-read source columns; both must reflect the
-- row at `_part_offset` derived from `__global_row_index`. Order by
-- `id` (rather than `_score`) to keep the reference free of
-- floating-point details.
SELECT id, s
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
ORDER BY id;

SELECT '-- 2. _part is emitted by the lazy reader (one entry per part)';
SELECT _part, count() FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
GROUP BY _part ORDER BY _part;

SELECT '-- 3. Empty main input → no output rows';
-- A `WHERE` predicate that selects no rows after the bitmap subquery
-- empties the main input; the lazy reader must produce zero rows.
SELECT count()
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 4)
WHERE id = 100;

SELECT '-- 4. Combined header drops __global_row_index';
-- `__global_row_index` is internal to the scorer side of the two-plan
-- structure; `LazyMaterializingTransform::transformHeader` drops it
-- before the join. Selecting `__global_row_index` from the table
-- function must therefore fail as an unknown identifier.
SELECT __global_row_index FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 1); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE tab;
