-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests the `keyValuePairs` tokenizer: a text index built directly on a Map(String, String) column,
-- which stores every (key, value) pair of a row as a single token and answers `m['key'] = 'value'`
-- with an exact token lookup and direct read.
--
-- Token layout: varint((length(key) << 1) | is_rest) || key || value
-- `is_rest` is 0 for the first occurrence of a key in a row and 1 for later duplicates, because
-- `m['key']` is the value of the *first* occurrence.

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

SELECT '-- the token of the level-error pair is char(5 * 2) followed by the key and then the value';
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab, idx) WHERE token = concat(char(10), 'level', 'error');

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

SELECT '-- the later duplicate is a distinct token: is_rest = 1 sets the lowest bit of the length prefix';
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_dup, idx);

DROP TABLE tab_dup;

