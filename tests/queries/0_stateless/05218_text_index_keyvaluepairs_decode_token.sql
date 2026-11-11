-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- EXPLAIN indexes output differs with parallel replicas

-- Tests decoding of the tokens of the `keyValuePairs` tokenizer: the `token_key` and `token_value` columns
-- of the table function `mergeTreeTextIndex` and the pairs shown in the description of the index condition.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET serialize_query_plan = 0; -- the trivial count step is not injected into a serialized plan

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab VALUES (1, {'level':'error','service':'api'}), (2, {'level':'warn','service':'api'}), (3, map('k', 'first', 'k', 'second')), (4, map('k', 'second'));

SELECT '-- the structure has the decoded parts of the token next to it';
DESCRIBE mergeTreeTextIndex(currentDatabase(), tab, idx);

SELECT '-- every token decodes back to its pair; the duplicate key gives the same pair as its first occurrence';
SELECT token_key, token_value, hex(token), cardinality FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

SELECT '-- the condition shows pairs instead of the encoded tokens';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] = 'error') WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] = 'warn' AND m['service'] = 'api') WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] = 'error' OR m['k'] = 'second') WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';

SELECT '-- so does the trivial count from the index';
SELECT trim(explain) FROM (EXPLAIN SELECT count() FROM tab WHERE m['level'] = 'error') WHERE explain LIKE '%Trivial count from text index%';
SELECT count() FROM tab WHERE m['level'] = 'error';

DROP TABLE tab;

SELECT '-- keys around the varint boundary (63 bytes: one trailer byte, 64 and 65 bytes: two), arbitrary bytes, empty key and value';

DROP TABLE IF EXISTS tab_bytes;

CREATE TABLE tab_bytes
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_bytes SELECT 1, map(repeat('a', 63), 'v63', repeat('b', 64), 'v64', repeat('c', 65), 'v65');
INSERT INTO tab_bytes VALUES (2, map('ab', 'c')), (3, map('a', 'bc')), (4, map('a\0b', 'x\0y')), (5, map('', '')), (6, map('k', '')), (7, map('\xFF', '\xFF')), (8, map('', 'ek')), (9, map('a"b', 'c\\d'));

SELECT length(token_key), hex(token_key), hex(token_value), length(token) FROM mergeTreeTextIndex(currentDatabase(), tab_bytes, idx) ORDER BY token;

SELECT '-- the decoded parts and the original pairs are the same sets';
SELECT arraySort(groupUniqArray((token_key, token_value))) = (SELECT arraySort(groupUniqArray((k, v))) FROM tab_bytes ARRAY JOIN mapKeys(m) AS k, mapValues(m) AS v)
FROM mergeTreeTextIndex(currentDatabase(), tab_bytes, idx);

SELECT '-- filtering on the decoded parts';
SELECT length(token_key), token_value FROM mergeTreeTextIndex(currentDatabase(), tab_bytes, idx) WHERE token_key = repeat('b', 64);
SELECT hex(token_key) FROM mergeTreeTextIndex(currentDatabase(), tab_bytes, idx) WHERE token_value = '' ORDER BY token;

SELECT '-- quotes, backslashes and control bytes in a pair are escaped in the descriptions';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_bytes WHERE m['a"b'] = 'c\\d') WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_bytes WHERE m['tab\tkey'] = 'new\nline') WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';
SELECT trim(explain) FROM (EXPLAIN SELECT count() FROM tab_bytes WHERE m['a"b'] = 'c\\d') WHERE explain LIKE '%Trivial count from text index%';

DROP TABLE tab_bytes;

SELECT '-- other tokenizers do not have the columns';

DROP TABLE IF EXISTS tab_text;

CREATE TABLE tab_text
(
    id UInt32,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_text VALUES (1, 'apple banana');

DESCRIBE mergeTreeTextIndex(currentDatabase(), tab_text, idx);
SELECT token_key FROM mergeTreeTextIndex(currentDatabase(), tab_text, idx); -- { serverError UNKNOWN_IDENTIFIER }
SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab_text WHERE hasToken(s, 'apple')) WHERE explain LIKE '%Condition%' AND explain NOT LIKE '%Condition: true%';
SELECT trim(explain) FROM (EXPLAIN SELECT count() FROM tab_text WHERE hasToken(s, 'apple')) WHERE explain LIKE '%Trivial count from text index%';

DROP TABLE tab_text;
