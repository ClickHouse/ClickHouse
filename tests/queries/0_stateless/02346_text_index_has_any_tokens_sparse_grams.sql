-- Test for issue #124156
-- `sparseGrams` compaction drops grams covered by a longer gram of the needle. That is valid only
-- under AND semantics (hasAllTokens). hasAnyTokens must match a row that contains only a covered gram.

SET explain_query_plan_default = 'legacy';
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    str String,
    INDEX idx str TYPE text(tokenizer = sparseGrams)
)
ENGINE = MergeTree ORDER BY id;

-- Row 2 shares only short grams with the needle (e.g. 'rts ', 'are '), all covered by longer grams.
INSERT INTO tab VALUES (1, 'Submit expense reports in the portal.'), (2, 'Reports are reviewed by finance.');

SELECT 'Array needle';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(str, tokens('when are expense reports due?', 'sparseGrams'));

SELECT 'String needle, index, direct read';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(str, 'when are expense reports due?')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT 'String needle, index, no direct read';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(str, 'when are expense reports due?')
SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;

SELECT 'String needle, no index';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(str, 'when are expense reports due?')
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'String needle, explicit tokenizer, no index';
SELECT groupArray(id) FROM tab WHERE hasAnyTokens(str, 'when are expense reports due?', 'sparseGrams')
SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'hasAllTokens is unaffected';
SELECT groupArray(id) FROM tab WHERE hasAllTokens(str, 'expense reports');
SELECT groupArray(id) FROM tab WHERE hasAllTokens(str, tokens('expense reports', 'sparseGrams'));

-- The hasAnyTokens index condition must keep the covered grams; hasAllTokens may drop them.
SELECT 'hasAnyTokens index condition';
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAnyTokens(str, 'abcdef')
)
WHERE explain LIKE '%Condition:%' AND explain LIKE '%mode:%';

SELECT 'hasAllTokens index condition';
SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT id FROM tab WHERE hasAllTokens(str, 'abcdef')
)
WHERE explain LIKE '%Condition:%' AND explain LIKE '%mode:%';

DROP TABLE tab;
