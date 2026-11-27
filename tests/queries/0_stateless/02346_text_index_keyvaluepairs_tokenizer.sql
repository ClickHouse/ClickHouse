-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests the `keyValuePairs` tokenizer: a text index built directly on a Map(String, String) column,
-- which stores every (key, value) pair of a row as a single token and answers `m['key'] = 'value'`
-- with an exact token lookup and direct read.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- One part with two granules: rows (1, 2) and rows (3, 4). Row 4 has no pairs at all.
INSERT INTO tab VALUES (1, {'level':'error','service':'api'}), (2, {'level':'warn','service':'api'}), (3, {'level':'error','service':'web'}), (4, {});

SELECT '-- exact key-value lookup';
SELECT id FROM tab WHERE m['level'] = 'error' ORDER BY id;
SELECT id FROM tab WHERE m['level'] = 'warn' ORDER BY id;
SELECT id FROM tab WHERE m['service'] = 'web' ORDER BY id;

SELECT '-- no match across pairs: the value of service is never the value of level';
SELECT count() FROM tab WHERE m['level'] = 'api';
SELECT '-- a key that does not exist';
SELECT count() FROM tab WHERE m['nope'] = 'error';

SELECT '-- token layout';
SELECT hex(token), cardinality FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY token;

SELECT '-- the token of the level-error pair is the key, the value, then char(5 * 2)';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab, idx) WHERE token = concat('level', 'error', char(10));

SELECT '-- the index prunes granules';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE m['level'] = 'warn') WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- exact direct read: the predicate is replaced by a text index virtual column';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] = 'error') WHERE explain LIKE '%__text_index_idx_equals%';

SELECT '-- both accessor forms work: arrayElement and the m.key_<key> subcolumn';
SELECT 'subcolumns=0', id FROM tab WHERE m['level'] = 'error' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'subcolumns=1', id FROM tab WHERE m['level'] = 'error' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'subcolumns=0', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] = 'error' SETTINGS optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%__text_index_idx_equals%';
SELECT 'subcolumns=1', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] = 'error' SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%__text_index_idx_equals%';

SELECT '-- AND / OR / NOT, with and without the index';
SELECT 'and idx', id FROM tab WHERE m['level'] = 'error' AND m['service'] = 'web' ORDER BY id;
SELECT 'and scan', id FROM tab WHERE m['level'] = 'error' AND m['service'] = 'web' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'or idx', id FROM tab WHERE m['level'] = 'warn' OR m['service'] = 'web' ORDER BY id;
SELECT 'or scan', id FROM tab WHERE m['level'] = 'warn' OR m['service'] = 'web' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not idx', id FROM tab WHERE NOT (m['level'] = 'error') ORDER BY id;
SELECT 'not scan', id FROM tab WHERE NOT (m['level'] = 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- comparing to the empty string is true for rows without the key too, and those have no token: the index is bypassed';
SELECT 'idx', id FROM tab WHERE m['level'] = '' ORDER BY id SETTINGS optimize_empty_string_comparisons = 0;
SELECT 'scan', id FROM tab WHERE m['level'] = '' ORDER BY id SETTINGS optimize_empty_string_comparisons = 0, use_skip_indexes = 0;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE m['level'] = '' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain LIKE '%__text_index%';

SELECT '-- a FixedString needle carries padding bytes, so it must not silently drop rows';
SELECT 'idx', groupArray(id) FROM (SELECT id FROM tab WHERE m['level'] = toFixedString('error', 8) ORDER BY id);
SELECT 'scan', groupArray(id) FROM (SELECT id FROM tab WHERE m['level'] = toFixedString('error', 8) ORDER BY id SETTINGS use_skip_indexes = 0);

DROP TABLE tab;

SELECT '-- duplicate keys: m[key] is the value of the first occurrence';

DROP TABLE IF EXISTS tab_dup;

CREATE TABLE tab_dup
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_dup VALUES (1, map('k', 'first', 'k', 'second')), (2, map('k', 'second'));

SELECT 'first idx', id FROM tab_dup WHERE m['k'] = 'first' ORDER BY id;
SELECT 'first scan', id FROM tab_dup WHERE m['k'] = 'first' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'second idx', id FROM tab_dup WHERE m['k'] = 'second' ORDER BY id;
SELECT 'second scan', id FROM tab_dup WHERE m['k'] = 'second' ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- the later duplicate is a distinct token: is_rest = 1 sets the lowest bit of the trailer';
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_dup, idx);

DROP TABLE tab_dup;

SELECT '-- key lengths around the varint boundary: 63 bytes still fit one byte, 64 need two';

DROP TABLE IF EXISTS tab_long;

CREATE TABLE tab_long
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab_long SELECT 1, map(repeat('a', 63), 'v63', repeat('b', 64), 'v64', repeat('c', 65), 'v65');

SELECT 'key 63', count() FROM tab_long WHERE m['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'] = 'v63';
SELECT 'key 64', count() FROM tab_long WHERE m['bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'] = 'v64';
SELECT 'key 65', count() FROM tab_long WHERE m['ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'] = 'v65';
SELECT 'no false match', count() FROM tab_long WHERE m['bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'] = 'v63';

SELECT 'varint 1 byte', count() FROM mergeTreeTextIndex(currentDatabase(), tab_long, idx) WHERE token = concat(repeat('a', 63), 'v63', char(126));
SELECT 'varint 2 bytes', count() FROM mergeTreeTextIndex(currentDatabase(), tab_long, idx) WHERE token = concat(repeat('b', 64), 'v64', char(1, 128));
SELECT 'varint 2 bytes', count() FROM mergeTreeTextIndex(currentDatabase(), tab_long, idx) WHERE token = concat(repeat('c', 65), 'v65', char(1, 130));

DROP TABLE tab_long;

SELECT '-- arbitrary bytes in keys and values, and empty keys and values';

DROP TABLE IF EXISTS tab_bytes;

CREATE TABLE tab_bytes
(
    id UInt32,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_bytes VALUES (1, map('ab', 'c')), (2, map('a', 'bc')), (3, map('a\0b', 'x\0y')), (4, map('', '')), (5, map('k', '')), (6, map('\xFF', '\xFF')), (7, map('', 'ek'));

SELECT '-- the trailing length keeps the pairs ab-c and a-bc apart: their tokens differ only in it';
SELECT id FROM tab_bytes WHERE m['ab'] = 'c' ORDER BY id;
SELECT id FROM tab_bytes WHERE m['a'] = 'bc' ORDER BY id;

SELECT '-- NUL bytes, high bytes and an empty key are all searchable';
SELECT id FROM tab_bytes WHERE m['a\0b'] = 'x\0y' ORDER BY id;
SELECT id FROM tab_bytes WHERE m['\xFF'] = '\xFF' ORDER BY id;
SELECT id FROM tab_bytes WHERE m[''] = 'ek' ORDER BY id;

SELECT '-- an empty value cannot be answered by the index, but the result must stay correct';
SELECT 'idx', count() FROM tab_bytes WHERE m['k'] = '' SETTINGS optimize_empty_string_comparisons = 0;
SELECT 'scan', count() FROM tab_bytes WHERE m['k'] = '' SETTINGS optimize_empty_string_comparisons = 0, use_skip_indexes = 0;

SELECT '-- every pair has a token, including the empty key and the empty value';
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_bytes, idx);

DROP TABLE tab_bytes;

SELECT '-- LowCardinality keys and values';

DROP TABLE IF EXISTS tab_lc;

CREATE TABLE tab_lc
(
    id UInt32,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_lc VALUES (1, {'level':'error'}), (2, {'level':'warn'});

SELECT 'idx', id FROM tab_lc WHERE m['level'] = 'error' ORDER BY id;
SELECT 'scan', id FROM tab_lc WHERE m['level'] = 'error' ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_lc, idx);

DROP TABLE tab_lc;

SELECT '-- a mix of LowCardinality and String is accepted';
DROP TABLE IF EXISTS tab_lc_mixed;
CREATE TABLE tab_lc_mixed (id UInt32, m Map(LowCardinality(String), String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id;
SELECT 'created';
DROP TABLE tab_lc_mixed;

SELECT '-- a part without a materialized index must not lose rows under exact direct read';

DROP TABLE IF EXISTS tab_partial;

CREATE TABLE tab_partial
(
    id UInt32,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab_partial;

-- This part predates the index, so the index is not materialized in it.
INSERT INTO tab_partial VALUES (1, {'level':'error'});

ALTER TABLE tab_partial ADD INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1;

-- This one is written with the index in place.
INSERT INTO tab_partial VALUES (2, {'level':'error'}), (3, {'level':'warn'});

SELECT 'some part has no materialized index', count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab_partial' AND active AND secondary_indices_marks_bytes = 0;
SELECT 'subcolumns=0', id FROM tab_partial WHERE m['level'] = 'error' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'subcolumns=1', id FROM tab_partial WHERE m['level'] = 'error' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'scan', id FROM tab_partial WHERE m['level'] = 'error' ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_partial;

SELECT '-- rejected index definitions';

DROP TABLE IF EXISTS tab_bad;

CREATE TABLE tab_bad (id UInt32, s String, INDEX idx s TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, UInt64), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(FixedString(2), String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, FixedString(2)), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, Nullable(String)), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', preprocessor = lower(m))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', postprocessor = lower(m))) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab_bad (id UInt32, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', support_phrase_search = 1)) ENGINE = MergeTree ORDER BY id SETTINGS allow_experimental_text_index_phrase_search = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- the tokenizer does not tokenize strings';
SELECT tokens('a b', 'keyValuePairs'); -- { serverError NOT_IMPLEMENTED }
