-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Tests `mapContainsKeyValue(m, 'key', 'value')` on a `keyValuePairs` text index: the pair may be the
-- key's first occurrence or a repetition, so both tokens are searched as one `Any` query, exact.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    m Map(String, String),
    v String,
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

-- One part with two granules: rows (1, 2) and rows (3, 4). Row 4 has no pairs at all.
INSERT INTO tab VALUES (1, {'level':'error','service':'api'}, 'error'), (2, {'level':'warn','service':'api'}, 'warn'), (3, {'level':'error','service':'web'}, 'x'), (4, {}, '');

SELECT '-- pair lookup';
SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', 'warn') ORDER BY id;
SELECT id FROM tab WHERE mapContainsKeyValue(m, 'service', 'web') ORDER BY id;

SELECT '-- no match across pairs: the value of service is never the value of level';
SELECT count() FROM tab WHERE mapContainsKeyValue(m, 'level', 'api');
SELECT '-- a key that does not exist';
SELECT count() FROM tab WHERE mapContainsKeyValue(m, 'nope', 'error');

SELECT '-- the index prunes granules';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', 'warn')) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

SELECT '-- exact direct read: the predicate is replaced by a text index virtual column';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', 'error')) WHERE explain LIKE '%__text_index_idx_mapContainsKeyValue%';

SELECT '-- AND / OR / NOT, with and without the index';
SELECT 'and idx', id FROM tab WHERE mapContainsKeyValue(m, 'level', 'error') AND mapContainsKeyValue(m, 'service', 'web') ORDER BY id;
SELECT 'and scan', id FROM tab WHERE mapContainsKeyValue(m, 'level', 'error') AND mapContainsKeyValue(m, 'service', 'web') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'or idx', id FROM tab WHERE mapContainsKeyValue(m, 'level', 'warn') OR mapContainsKeyValue(m, 'service', 'web') ORDER BY id;
SELECT 'or scan', id FROM tab WHERE mapContainsKeyValue(m, 'level', 'warn') OR mapContainsKeyValue(m, 'service', 'web') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not idx', id FROM tab WHERE NOT mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT 'not scan', id FROM tab WHERE NOT mapContainsKeyValue(m, 'level', 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

SELECT '-- needles the index cannot search must keep the result correct';
SELECT 'fixed string idx', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, toFixedString('level', 8), 'error') ORDER BY id);
SELECT 'fixed string scan', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, toFixedString('level', 8), 'error') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT 'fixed string not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE mapContainsKeyValue(m, toFixedString('level', 8), 'error')) WHERE explain LIKE '%__text_index%';
SELECT 'column idx', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', v) ORDER BY id);
SELECT 'column scan', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', v) ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT 'column not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', v)) WHERE explain LIKE '%__text_index%';

SELECT '-- the same rows whether or not the needle is recognized as a constant';
SELECT 'materialize idx', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', materialize('error')) ORDER BY id);
SELECT 'materialize scan', groupArray(id) FROM (SELECT id FROM tab WHERE mapContainsKeyValue(m, 'level', materialize('error')) ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT 'null idx', count() FROM tab WHERE mapContainsKeyValue(m, 'level', NULL);
SELECT 'null scan', count() FROM tab WHERE mapContainsKeyValue(m, 'level', NULL) SETTINGS use_skip_indexes = 0;

DROP TABLE tab;

SELECT '-- a repeated key matches on any of its occurrences, unlike m[key]';

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

INSERT INTO tab_dup VALUES (1, map('k', 'first', 'k', 'second')), (2, map('k', 'second')), (3, map('k', ''));

SELECT 'first idx', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', 'first') ORDER BY id;
SELECT 'first scan', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', 'first') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'second idx', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', 'second') ORDER BY id;
SELECT 'second scan', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', 'second') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'second element', id FROM tab_dup WHERE m['k'] = 'second' ORDER BY id;

SELECT '-- the repeated occurrence has its own token and both are searched';
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_dup, idx);
SELECT arraySort(groupUniqArray(hex(token))) FROM mergeTreeTextIndex(currentDatabase(), tab_dup, idx) WHERE token IN (concat('k', 'second', char(2)), concat('k', 'second', char(3)));

SELECT '-- an empty value is a pair like any other, so the index answers it';
SELECT 'empty idx', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', '') ORDER BY id;
SELECT 'empty scan', id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', '') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'empty replaced', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_dup WHERE mapContainsKeyValue(m, 'k', '')) WHERE explain LIKE '%__text_index_idx_mapContainsKeyValue%';

DROP TABLE tab_dup;

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

SELECT 'idx', id FROM tab_lc WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT 'scan', id FROM tab_lc WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_lc;

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
SELECT 'idx', id FROM tab_partial WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT 'scan', id FROM tab_partial WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE tab_partial;

SELECT '-- an index on mapKeys or mapValues cannot answer a pair lookup';

DROP TABLE IF EXISTS tab_keys;

CREATE TABLE tab_keys
(
    id UInt32,
    m Map(String, String),
    INDEX idx_keys mapKeys(m) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX idx_values mapValues(m) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO tab_keys VALUES (1, {'level':'error'}), (2, {'level':'warn'}), (3, {'service':'error'});

SELECT 'idx', id FROM tab_keys WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT 'scan', id FROM tab_keys WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT 'not replaced', count() FROM (EXPLAIN actions = 1 SELECT id FROM tab_keys WHERE mapContainsKeyValue(m, 'level', 'error')) WHERE explain LIKE '%__text_index%';

DROP TABLE tab_keys;
