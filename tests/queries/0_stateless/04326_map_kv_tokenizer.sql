-- Verify the keyValuePairs tokenizer is registered.
SELECT name FROM system.tokenizers WHERE name = 'keyValuePairs';

DROP TABLE IF EXISTS t_map_kv;
CREATE TABLE t_map_kv
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO t_map_kv VALUES (1, {'foo':'bar'}), (2, {'foo':'baz','k2':'v2'});

-- Each distinct pair becomes one token: key || value || chr(length(key)).
SELECT hex(token), cardinality FROM mergeTreeTextIndex(currentDatabase(), t_map_kv, idx) ORDER BY token;

-- The exact 'foo' -> 'bar' token is present (length of key 'foo' is 3).
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_map_kv, idx) WHERE token = concat('foo', 'bar', char(3));

-- Exact key-value lookups answered by the index.
SELECT id FROM t_map_kv WHERE m['foo'] = 'bar' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['foo'] = 'baz' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['k2'] = 'v2' ORDER BY id;
-- No false match across pairs: 'foo' never maps to 'v2', unknown key 'nope' matches nothing.
SELECT id FROM t_map_kv WHERE m['foo'] = 'v2' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['nope'] = 'bar' ORDER BY id;

-- The text index is used for the equality.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE m['foo'] = 'bar') WHERE explain LIKE '%Name: idx%';

-- Value-only search, resolved by a decode-aware dictionary scan.
SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'baz') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsValueLike(m, 'v%') ORDER BY id;
-- Key-scoped value prefix (m['key'] LIKE 'p%' is rewritten to startsWith).
SELECT id FROM t_map_kv WHERE m['foo'] LIKE 'ba%' ORDER BY id;
SELECT id FROM t_map_kv WHERE startsWith(m['k2'], 'v') ORDER BY id;

-- The text index is used for value-only search too.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar')) WHERE explain LIKE '%Name: idx%';

-- Value-only search also engages direct read (a text-index virtual column replaces the predicate).
-- Pin query_plan_direct_read_from_text_index: the flaky check randomizes it, and with direct read
-- off the __text_index_ virtual column is not emitted, so this plan-shape assertion is optional-opt sensitive.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar') SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Key search: existence and key LIKE, resolved by the dictionary scan.
SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'foo') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'k2') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsKeyLike(m, 'k%') ORDER BY id;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'foo')) WHERE explain LIKE '%Name: idx%';

-- Map default-value semantics: m['absent'] is '', so a predicate that is true on '' must still
-- match rows lacking the key (which have no token). Such predicates fall back to a full scan.
DROP TABLE IF EXISTS t_map_kv_def;
CREATE TABLE t_map_kv_def
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_def VALUES (1, {'foo':'bar'}), (2, {'x':'y'}), (3, {'foo':''});
-- Rows without 'foo' (default '') and 'foo' -> '' both match.
SELECT id FROM t_map_kv_def WHERE m['foo'] = '' ORDER BY id;
-- 'm[key] LIKE ''%''' matches every row, including those lacking the key.
SELECT id FROM t_map_kv_def WHERE m['foo'] LIKE '%' ORDER BY id;
-- Non-empty needles remain precise.
SELECT id FROM t_map_kv_def WHERE m['foo'] = 'bar' ORDER BY id;
DROP TABLE t_map_kv_def;

-- LowCardinality(String) keys and values are supported (the dictionary values are strings).
DROP TABLE IF EXISTS t_map_kv_lc;
CREATE TABLE t_map_kv_lc
(
    id UInt64,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_lc VALUES (1, {'level':'error'}), (2, {'level':'info','svc':'api'});
SELECT id FROM t_map_kv_lc WHERE m['level'] = 'error' ORDER BY id;
SELECT id FROM t_map_kv_lc WHERE mapContainsValue(m, 'api') ORDER BY id;
DROP TABLE t_map_kv_lc;

-- The keyValuePairs tokenizer requires a Map with String or LowCardinality(String) keys/values.
CREATE TABLE t_map_kv_bad2 (id UInt64, m Map(String, UInt64), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- The keyValuePairs tokenizer requires a Map column.
CREATE TABLE t_map_kv_bad (id UInt64, s String, INDEX idx s TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- FixedString keys/values are rejected: the query-side key is unpadded while the index stores
-- the padded FixedString bytes, so lookups would silently miss rows.
CREATE TABLE t_map_kv_bad_fs (id UInt64, m Map(FixedString(3), String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_map_kv_bad_fsv (id UInt64, m Map(String, FixedString(3)), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- The keyValuePairs tokenizer does not support the preprocessor / postprocessor options.
CREATE TABLE t_map_kv_bad3 (id UInt64, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', preprocessor = 'toString(m)')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_map_kv_bad4 (id UInt64, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', postprocessor = 'lower(token)')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Two literal (wildcard-free) patterns in one predicate must not be conflated (distinct query hashes).
SELECT id FROM t_map_kv WHERE mapContainsValueLike(m, 'bar') OR mapContainsValueLike(m, 'v2') ORDER BY id;
SELECT id FROM t_map_kv WHERE m['foo'] LIKE 'bar' OR m['foo'] LIKE 'baz' ORDER BY id;

DROP TABLE t_map_kv;

-- A value-matcher query (e.g. mapContainsValue) discovers its tokens dynamically and must not be
-- failed by the first clipped token. PK filtering (id >= 7) prunes the granule holding the
-- earlier-sorted token ('k2' -> 'x' from row 1), making it unreadable; the query must still match
-- row 8 via its own ('k' -> 'x') token instead of reporting a false negative (empty result).
DROP TABLE IF EXISTS t_map_kv_clip;
CREATE TABLE t_map_kv_clip
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_clip VALUES
    (1, {'k2':'x'}), (2, {'z':'q'}), (3, {'z':'q'}), (4, {'z':'q'}),
    (5, {'z':'q'}), (6, {'z':'q'}), (7, {'z':'q'}), (8, {'k':'x'});
SELECT id FROM t_map_kv_clip WHERE id >= 7 AND mapContainsValue(m, 'x') ORDER BY id;
DROP TABLE t_map_kv_clip;
