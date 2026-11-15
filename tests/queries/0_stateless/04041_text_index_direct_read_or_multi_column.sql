-- Regression test for a bug where OR of hasAllTokens on different columns
-- with separate text indexes returned incorrect results.
-- The bug was in MergeTreeReaderTextIndex::fillColumn: when postings.size() == 1
-- it incorrectly used applyPostingsAny instead of applyPostingsAll, ignoring
-- missing tokens in the postings map.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS texttmp2;

CREATE TABLE texttmp2
(
    `id` UInt32,
    `title` String,
    `content` String,
    INDEX content_idx content TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 100000000,
    INDEX title_idx title TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO texttmp2 VALUES(1, 'qqa rerer', 'qqa rerer');

-- Test 1: OR of hasAllTokens on different columns, neither side matches.
-- title does not contain 'abc' and 'def'; content does not contain both 'xyz' and 'qqa'.
SELECT 'Test 1 OR neither matches:', count()
FROM texttmp2
WHERE hasAllTokens(title, ['abc', 'def']) OR hasAllTokens(content, ['xyz', 'qqa']);

-- Test 2: Only the left side of OR matches.
SELECT 'Test 2 OR left matches:', count()
FROM texttmp2
WHERE hasAllTokens(title, ['qqa', 'rerer']) OR hasAllTokens(content, ['xyz', 'abc']);

-- Test 3: Only the right side of OR matches.
SELECT 'Test 3 OR right matches:', count()
FROM texttmp2
WHERE hasAllTokens(title, ['abc', 'def']) OR hasAllTokens(content, ['qqa', 'rerer']);

-- Test 4: Both sides of OR match.
SELECT 'Test 4 OR both match:', count()
FROM texttmp2
WHERE hasAllTokens(title, ['qqa', 'rerer']) OR hasAllTokens(content, ['qqa', 'rerer']);

-- Test 5: One token present, one missing in hasAllTokens (the exact bug scenario).
SELECT 'Test 5 partial token match:', count()
FROM texttmp2
WHERE hasAllTokens(content, ['xyz', 'qqa']);

-- Test 6: hasAnyTokens with OR (should work correctly).
SELECT 'Test 6 hasAnyTokens OR:', count()
FROM texttmp2
WHERE hasAnyTokens(title, ['abc', 'def']) OR hasAnyTokens(content, ['xyz', 'qqa']);

DROP TABLE texttmp2;
