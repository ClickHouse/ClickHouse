-- Regression test: TextSearchQuery::getHash must distinguish queries whose tokens
-- have the same concatenated bytes but different boundaries.
-- E.g. tokens ['abc', 'def'] vs ['abcd', 'ef'] both concatenate to "abcdef"
-- but represent different search conditions. A collision would cause one query's
-- virtual column to be mapped to the wrong search tokens in direct-read mode.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_hash;

CREATE TABLE tab_hash (
    id UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 64;

-- Token structure:
-- Group A: tokens 'abc' and 'def' (but NOT 'abcd' or 'ef')
-- Group B: tokens 'abcd' and 'ef' (but NOT 'abc' or 'def')
-- Group C: has all four tokens
INSERT INTO tab_hash SELECT number, concat('abc def row ', toString(number)) FROM numbers(100);
INSERT INTO tab_hash SELECT 100 + number, concat('abcd ef row ', toString(number)) FROM numbers(100);
INSERT INTO tab_hash SELECT 200 + number, concat('abc def abcd ef row ', toString(number)) FROM numbers(100);

-- Test 1: hasAllTokens with ['abc', 'def'] should match groups A and C (200 rows).
SELECT 'Test 1 abc+def:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['abc', 'def']);

-- Test 2: hasAllTokens with ['abcd', 'ef'] should match groups B and C (200 rows).
SELECT 'Test 2 abcd+ef:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['abcd', 'ef']);

-- Test 3: Both conditions in one query via OR.
-- Without the fix, the two hasAllTokens calls could hash-collide,
-- causing one to be mapped to the other's tokens. With the fix,
-- each gets its own virtual column and returns correct results.
-- Groups A, B, and C all match at least one condition (300 rows).
SELECT 'Test 3 OR both:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['abc', 'def']) OR hasAllTokens(text, ['abcd', 'ef']);

-- Test 4: AND of the two conditions should match only group C (100 rows).
SELECT 'Test 4 AND both:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['abc', 'def']) AND hasAllTokens(text, ['abcd', 'ef']);

-- Test 5: Single-token boundary difference: ['a', 'bcd'] vs ['ab', 'cd'].
-- Group D: has 'a' and 'bcd' but not 'ab' or 'cd'
-- Group E: has 'ab' and 'cd' but not 'a' or 'bcd'
TRUNCATE TABLE tab_hash;
INSERT INTO tab_hash SELECT number, concat('a bcd row ', toString(number)) FROM numbers(100);
INSERT INTO tab_hash SELECT 100 + number, concat('ab cd row ', toString(number)) FROM numbers(100);

SELECT 'Test 5 a+bcd:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['a', 'bcd']);

SELECT 'Test 6 ab+cd:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['ab', 'cd']);

-- Test 7: Both in one OR - should get 200 (all rows).
SELECT 'Test 7 OR boundary:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['a', 'bcd']) OR hasAllTokens(text, ['ab', 'cd']);

-- Test 8: AND - should get 0 (no row has all four tokens).
SELECT 'Test 8 AND boundary:', count()
FROM tab_hash
WHERE hasAllTokens(text, ['a', 'bcd']) AND hasAllTokens(text, ['ab', 'cd']);

DROP TABLE tab_hash;
