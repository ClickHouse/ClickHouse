-- Regression test: text index with preprocessor must apply the preprocessor
-- to set values in IN conditions. Previously, the preprocessor (e.g. lower())
-- was not applied to IN-set elements, causing false-negative granule skips
-- when the index stores lowered tokens but the query searches for mixed-case values.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_in_preprocess;

-- Index with lower() preprocessor and splitByNonAlpha tokenizer.
-- The index stores tokens in lowercase (e.g. "hello", "world").
CREATE TABLE tab_in_preprocess (
    id UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(text)) GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 64;

INSERT INTO tab_in_preprocess SELECT number, concat('Hello World row ', toString(number)) FROM numbers(100);
INSERT INTO tab_in_preprocess SELECT 100 + number, concat('Goodbye Planet row ', toString(number)) FROM numbers(100);
INSERT INTO tab_in_preprocess SELECT 200 + number, concat('Nothing here row ', toString(number)) FROM numbers(100);

-- Test 1: Basic IN with mixed-case value that matches after lowering.
-- The index stores "hello" (lowered). The IN set has 'Hello World' which
-- should be lowered to 'hello world' and tokenized to ['hello', 'world'].
-- Without the fix, the raw 'Hello' token would not match 'hello' in the index,
-- causing incorrect granule skipping and returning 0 rows instead of 100.
SELECT 'Test 1 IN with preprocessor:', count()
FROM tab_in_preprocess
WHERE text IN ('Hello World row 0', 'Hello World row 1');

-- Test 2: equals() with mixed-case value (this already worked before the fix).
SELECT 'Test 2 equals:', count()
FROM tab_in_preprocess
WHERE text = 'Hello World row 0';

-- Test 3: IN with multiple mixed-case values from different groups.
SELECT 'Test 3 IN multiple groups:', count()
FROM tab_in_preprocess
WHERE text IN ('Hello World row 0', 'Goodbye Planet row 0');

-- Test 4: NOT IN should also work correctly.
-- All 300 rows minus the 2 matching ones.
SELECT 'Test 4 NOT IN:', count()
FROM tab_in_preprocess
WHERE text NOT IN ('Hello World row 0', 'Goodbye Planet row 0');

-- Test 5: IN with tuple (key position detection).
SELECT 'Test 5 IN tuple:', count()
FROM tab_in_preprocess
WHERE (id, text) IN ((0, 'Hello World row 0'), (100, 'Goodbye Planet row 0'));

DROP TABLE tab_in_preprocess;
