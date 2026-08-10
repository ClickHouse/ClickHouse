-- Tags: no-fasttest

-- The lazy column-proxy reader of the scored-search table functions is the
-- only step that reads the source columns, so its reads must be accounted
-- against `max_rows_to_read`. The bitmap prefilter scan has its own
-- accounting (covered elsewhere); this test pins down the lazy branch:
-- without a WHERE clause the only reads are the scorer's top-K output
-- (5 rows here) and the lazy granule-aligned read of the source column
-- (>= one 256-row granule), so a limit between the two trips only if the
-- lazy read is accounted.

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
-- Pin the granularity settings (the test runner randomizes them): with
-- ~30-byte rows the bytes threshold is never reached, so granules hold
-- exactly 256 rows and the lazy read of the top-5 rows reads one granule.
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 256, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, [toFloat32(number), 0.0] FROM numbers(1000);

SELECT '-- tight limit trips on the lazy source-column read';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 5)
SETTINGS max_rows_to_read = 50; -- { serverError TOO_MANY_ROWS }

SELECT '-- generous limit reads fine';
SELECT count() FROM
(
    SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 5)
)
SETTINGS max_rows_to_read = 100000;

DROP TABLE tab;
