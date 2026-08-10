-- Tags: no-fasttest

-- Plan 01 §"Tests": `BuildBitmapsTransform` enforces
-- `search_topk_prefilter_max_rows` and throws `LIMIT_EXCEEDED` when the
-- prefilter would hold more rows than the budget. No fallback path
-- exists; this test asserts the fail-loud contract.

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

-- A `payload >= 0` predicate selects all 5000 rows; the bitmap
-- subquery emits one `_part_offset` per matching row, so the prefilter
-- holds 5000 rows — far above the 100-row budget below. Kept small:
-- the INSERT builds an HNSW index for every row, which is slow under
-- sanitizers (a 50000-row variant timed out in the flaky check).
INSERT INTO tab SELECT number, number, [toFloat32(number), 0.0] FROM numbers(5000);

-- 100 rows is far below the 5000 surviving rows; the bitmap subquery
-- must throw `LIMIT_EXCEEDED`.
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3)
WHERE payload >= 0
SETTINGS search_topk_prefilter_max_rows = 100; -- { serverError LIMIT_EXCEEDED }

DROP TABLE tab;
