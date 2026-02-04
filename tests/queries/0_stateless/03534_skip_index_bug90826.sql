-- Test for https://github.com/ClickHouse/ClickHouse/issues/90826
-- This test verifies that skip indexes (bloom_filter and text index) work correctly
-- when the last granule contains fewer rows than `index_granularity` (default 8192).
-- This used to return wrong results because the index reader would read more rows
-- than actually exist in the last granule. This could crash the process.

SET use_skip_indexes_on_data_read = 1;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

-- Test Case 1: bloom_filter index with last granule containing 2 rows (8194 - 8192 = 2).
-- This used to crash.

CREATE TABLE tab
(
    `i` Int64,
    `text` String,
    INDEX bf_text text TYPE bloom_filter(0.001) GRANULARITY 1,
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Insert 8194 rows: first granule has 8192 rows, last granule has only 2 rows
INSERT INTO tab SELECT number, toString(number) FROM numbers(8194);

-- Query for row 8192 which is in the last (incomplete) granule
SELECT i, text FROM tab WHERE text = '8192';

DROP TABLE tab;

-- Test Case 2: text index (full-text index) with last granule containing 1 row (8193 - 8192 = 1).
-- This used to crash.

CREATE TABLE tab
(
    `i` Int64,
    `text` String,
    INDEX inv_text text TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

-- Insert 8193 rows: first granule has 8192 rows, last granule has only 1 row
INSERT INTO tab SELECT number, toString(number) FROM numbers(8193);

-- Query for row 8192 which is the only row in the last (incomplete) granule
SELECT i, text FROM tab WHERE hasToken(text, '8192');

DROP TABLE tab;
