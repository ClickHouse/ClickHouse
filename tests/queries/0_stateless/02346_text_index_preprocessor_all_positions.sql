-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

SELECT 'the occurrence agrees in every position and under every read-path setting';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByString(['|']), preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'A b|c'), (2, 'a b|c'), (3, 'zzz');

SELECT groupArray(id) FROM tab WHERE hasAnyTokens(s, ['a b']);
SELECT groupArray(id) FROM tab PREWHERE hasAnyTokens(s, ['a b']);
SELECT id, hasAnyTokens(s, ['a b']) FROM tab ORDER BY id;
SELECT countIf(hasAnyTokens(s, ['a b'])) FROM tab;

SELECT count() FROM tab WHERE hasAnyTokens(s, ['a b']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, ['a b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab WHERE hasAnyTokens(s, ['a b']) SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, ['a b']) SETTINGS ignore_data_skipping_indices = 'idx';

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (
    EXPLAIN actions = 1
    SELECT hasAnyTokens(s, ['a b']) FROM tab
) WHERE explain ILIKE '%hasAnyTokens%';

DROP TABLE tab;

SELECT 'hasToken, hasAllTokens and hasPhrase behave the same way';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'FOO bar'), (2, 'foo bar'), (3, 'zzz');

SELECT groupArray(id) FROM tab WHERE hasToken(s, 'foo') SETTINGS use_skip_indexes = 0;
SELECT id, hasToken(s, 'foo') FROM tab ORDER BY id;
SELECT groupArray(id) FROM tab WHERE hasAllTokens(s, 'foo bar') SETTINGS use_skip_indexes = 0;
SELECT id, hasAllTokens(s, 'foo bar') FROM tab ORDER BY id;
SELECT groupArray(id) FROM tab WHERE hasPhrase(s, 'foo bar') SETTINGS use_skip_indexes = 0;
SELECT id, hasPhrase(s, 'foo bar') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'a lightweight update on the indexed column keeps the preprocessor applied';

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, preprocessor = lower(s))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO tab VALUES (1, 'Beta marker'), (2, 'Gamma filler');

SELECT count() FROM tab WHERE hasToken(s, 'beta');

UPDATE tab SET s = s WHERE id = 2;

SELECT count() FROM tab WHERE hasToken(s, 'beta');

UPDATE tab SET s = 'Delta' WHERE id = 1;

SELECT count() FROM tab WHERE hasToken(s, 'beta');

DROP TABLE tab;
