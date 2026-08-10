-- Tags: no-fasttest

-- The bitmap prefilter subquery is a hidden scan of the source table, so it
-- must be accounted against the read limits of the main query
-- (`max_rows_to_read` and friends), like a normal source-table read.

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

INSERT INTO tab SELECT number, number % 2, [number, 0.0] FROM numbers(8);

SELECT '-- Generous limit: the query succeeds';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 2)
WHERE payload = 1
ORDER BY id
SETTINGS max_rows_to_read = 100;

SELECT '-- The prefilter scan of 8 rows trips a lower limit';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 2)
WHERE payload = 1
ORDER BY id
SETTINGS max_rows_to_read = 4; -- { serverError TOO_MANY_ROWS }

DROP TABLE tab;
