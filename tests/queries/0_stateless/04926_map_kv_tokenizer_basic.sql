-- Basic feature completeness for the keyValuePairs text-index tokenizer on Map(String, String)
-- columns: static value combinations, single predicates. Compound expressions live in
-- 04727_map_kv_tokenizer_compound; regression tests for specific bugs live in
-- 04728_map_kv_tokenizer_bugfixes; randomized consistency checks live in the two .sh tests.

-- ============================================================================
-- Registration, token format, and exact key-value lookups.
-- ============================================================================

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

-- Each distinct pair becomes one token: key || value || chr((length(key) << 1) | is_rest). The trailer
-- packs the per-key occurrence flag in the low bit; a first (here only) occurrence has is_rest = 0, so
-- the trailer byte is length(key) * 2.
SELECT hex(token), cardinality FROM mergeTreeTextIndex(currentDatabase(), t_map_kv, idx) ORDER BY token;

-- The exact 'foo' -> 'bar' token is present (key 'foo' has length 3, first occurrence -> trailer 3*2 = 6).
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_map_kv, idx) WHERE token = concat('foo', 'bar', char(6));

-- Exact key-value lookups answered by the index.
SELECT id FROM t_map_kv WHERE m['foo'] = 'bar' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['foo'] = 'baz' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['k2'] = 'v2' ORDER BY id;
-- No false match across pairs: 'foo' never maps to 'v2', unknown key 'nope' matches nothing.
SELECT id FROM t_map_kv WHERE m['foo'] = 'v2' ORDER BY id;
SELECT id FROM t_map_kv WHERE m['nope'] = 'bar' ORDER BY id;

-- The text index is used for the equality.
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE m['foo'] = 'bar') WHERE explain LIKE '%Name: idx%';

-- Value-only search, resolved by a decode-aware dictionary scan.
SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'baz') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsValueLike(m, 'v%') ORDER BY id;
-- Key-scoped value prefix (m['key'] LIKE 'p%' is rewritten to startsWith).
SELECT id FROM t_map_kv WHERE m['foo'] LIKE 'ba%' ORDER BY id;
SELECT id FROM t_map_kv WHERE startsWith(m['k2'], 'v') ORDER BY id;

-- The text index is used for value-only search too.
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar')) WHERE explain LIKE '%Name: idx%';

-- Value-only search also engages direct read (a text-index virtual column replaces the predicate).
-- Pin query_plan_direct_read_from_text_index: the flaky check randomizes it, and with direct read
-- off the __text_index_ virtual column is not emitted, so this plan-shape assertion is optional-opt sensitive.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv WHERE mapContainsValue(m, 'bar') SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Key search: existence and key LIKE, resolved by the dictionary scan.
SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'foo') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'k2') ORDER BY id;
SELECT id FROM t_map_kv WHERE mapContainsKeyLike(m, 'k%') ORDER BY id;
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_map_kv WHERE mapContainsKey(m, 'foo')) WHERE explain LIKE '%Name: idx%';

DROP TABLE t_map_kv;

-- ============================================================================
-- Map default-value semantics: m['absent'] is '', so a predicate that is true on '' must still
-- match rows lacking the key (which have no token). Such predicates fall back to a full scan.
-- ============================================================================
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

-- ============================================================================
-- LowCardinality(String) keys and values are supported (the dictionary values are strings).
-- ============================================================================
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

-- ============================================================================
-- Rejected column types and options.
-- ============================================================================
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

-- ============================================================================
-- LIKE rewrites: the dictionary-scan opt-in (use_text_index_like_evaluation_by_dictionary_scan) and
-- the minimum literal length (text_index_like_min_pattern_length) gate all keyValuePairs LIKE forms
-- (mapContainsValueLike, mapContainsKeyLike, m['key'] LIKE, startsWith/endsWith). Results are always
-- correct regardless of the setting (a disabled/too-short pattern just falls back to a full scan).
-- ============================================================================
DROP TABLE IF EXISTS t_map_kv_like;
CREATE TABLE t_map_kv_like
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_map_kv_like VALUES (1, {'level':'error'}), (2, {'level':'info'}), (3, {'svc':'errand'});

-- Results are independent of the dictionary-scan setting.
-- prefix 'erro%' -> 'error' (row 1); literal 'error' -> row 1; substring '%rror%' -> row 1; key prefix 'leve%' -> rows 1,2.
SELECT 'prefix', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'prefix', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'literal', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'literal', id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;
SELECT 'keyprefix', id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1;
SELECT 'keyprefix', id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') ORDER BY id SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0;

-- The __text_index_ virtual column (direct read) is present for every pattern shape when the scan is
-- enabled and the literal is long enough. query_plan_direct_read_from_text_index is pinned so the
-- assertion tests the LIKE gate, not the direct-read toggle.
SELECT 'valueLike prefix on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike literal on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'error') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'valueLike substring on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'keyLike prefix on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'elementLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%rror%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Disabled dictionary scan: no direct read for any shape.
SELECT 'valueLike prefix off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'erro%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'keyLike prefix off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsKeyLike(m, 'leve%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- Literal shorter than text_index_like_min_pattern_length: no direct read even with the scan enabled.
SELECT 'valueLike short', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE mapContainsValueLike(m, 'err%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

-- m['key'] LIKE 'p%' is rewritten to startsWith(m['key'], 'p') and m['key'] LIKE '%s' to
-- endsWith(m['key'], 's'); both are key-scoped value prefix/suffix searches over the decoded token,
-- and both obey the same dictionary-scan opt-in and minimum literal length gate as the other LIKE forms.
-- Results are always correct regardless of the setting (a disabled/too-short pattern just falls back).
SELECT 'prefix erro%', id FROM t_map_kv_like WHERE m['level'] LIKE 'erro%' ORDER BY id;
SELECT 'suffix %rror', id FROM t_map_kv_like WHERE m['level'] LIKE '%rror' ORDER BY id;
SELECT 'startsWith erro', id FROM t_map_kv_like WHERE startsWith(m['level'], 'erro') ORDER BY id;
SELECT 'endsWith rror', id FROM t_map_kv_like WHERE endsWith(m['level'], 'rror') ORDER BY id;
-- Short prefix/suffix (below the minimum): still correct via the full-scan fallback.
SELECT 'prefix short er%', id FROM t_map_kv_like WHERE m['level'] LIKE 'er%' ORDER BY id;
SELECT 'suffix short %or', id FROM t_map_kv_like WHERE m['level'] LIKE '%or' ORDER BY id;

-- Direct read for startsWith/endsWith obeys the same gate as the other LIKE rewrites, but the key is a
-- fully specified prefix of every matched token (`key || value || trailer`), so the selectivity bound is
-- len(key) + len(prefix/suffix), not the value part alone. Key 'level' has length 5.
-- A value part alone below the minimum still qualifies once the key is counted (5 + 2 >= 4).
SELECT 'startsWith key-qualifies', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE 'er%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'endsWith key-qualifies', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%or' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 4, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
-- len(key) + len(value) below the minimum (5 + 2 < 10): no direct read, full-scan fallback.
SELECT 'startsWith below min', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE 'er%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'endsWith below min', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE '%or' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
-- Dictionary scan disabled: no direct read even for a long prefix.
SELECT 'startsWith scan off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_kv_like WHERE m['level'] LIKE 'erro%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

DROP TABLE t_map_kv_like;

-- ============================================================================
-- Long keys: the token trailer encodes (length(key) << 1) | is_rest, so a key shorter than 64 bytes
-- fits one trailer byte and a key of 64 bytes or more takes the multi-byte reversed-varint path. The
-- index must answer exactly like a plain scan for all key/value lengths. (The short-token multi-byte
-- trailer boundary that once mis-decoded is exercised in 04728_map_kv_tokenizer_bugfixes.)
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
-- key lengths: 127 (last single-byte trailer), 128 (first multi-byte trailer), 200 (multi-byte) with a
-- 300-byte value, and a long key sharing a value with another row.
INSERT INTO t_mem VALUES
    (1, map(repeat('k', 127), 'v1')),
    (2, map(repeat('K', 128), 'v2')),
    (3, map(repeat('x', 200), repeat('y', 300))),
    (4, map(repeat('K', 128), 'shared')),
    (5, map('short', 'shared'));
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- exact m[key] = value --';
SELECT id FROM t_mem WHERE m[repeat('k', 127)] = 'v1' ORDER BY id;
SELECT id FROM t_idx WHERE m[repeat('k', 127)] = 'v1' ORDER BY id;
SELECT id FROM t_mem WHERE m[repeat('K', 128)] = 'v2' ORDER BY id;
SELECT id FROM t_idx WHERE m[repeat('K', 128)] = 'v2' ORDER BY id;
SELECT id FROM t_mem WHERE m[repeat('x', 200)] = repeat('y', 300) ORDER BY id;
SELECT id FROM t_idx WHERE m[repeat('x', 200)] = repeat('y', 300) ORDER BY id;

SELECT '-- mapContainsKey (long key) --';
SELECT id FROM t_mem WHERE mapContainsKey(m, repeat('K', 128)) ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKey(m, repeat('K', 128)) ORDER BY id;

SELECT '-- mapContainsValue (long value, and value shared by long+short key) --';
SELECT id FROM t_mem WHERE mapContainsValue(m, repeat('y', 300)) ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsValue(m, repeat('y', 300)) ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsValue(m, 'shared') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsValue(m, 'shared') ORDER BY id;

SELECT '-- no false match on a long absent key/value --';
SELECT id FROM t_mem WHERE m[repeat('K', 128)] = 'nope' ORDER BY id;
SELECT id FROM t_idx WHERE m[repeat('K', 128)] = 'nope' ORDER BY id;
SELECT count() FROM t_idx WHERE mapContainsKey(m, repeat('z', 128));

DROP TABLE t_mem;
DROP TABLE t_idx;

-- ============================================================================
-- mapContainsKeyValue / mapContainsKeyValueLike are existence predicates over map (key, value) pairs.
-- Unlike m['key'] = value (which is the first value for the key), they match any occurrence, so they
-- are well-defined for duplicate keys. The keyValuePairs index answers them (exact token for the pair,
-- dictionary scan for the LIKE form); results must equal a plain scan.
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mem VALUES (1, {'level':'error'}), (2, {'level':'info','svc':'api'}), (3, {'k':'a','k':'b'});
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- mapContainsKeyValue: indexed == Memory --';
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'error') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'svc', 'api') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'svc', 'api') ORDER BY id;
-- Duplicate key: the pair (k, b) exists (second occurrence), so existence matches row 3 —
-- this is exactly where it differs from m['k'] = 'b' (first value is 'a').
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'k', 'b') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'k', 'b') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValue(m, 'level', 'nope') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'nope') ORDER BY id;

SELECT '-- mapContainsKeyValueLike: indexed == Memory --';
SELECT id FROM t_mem WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') ORDER BY id;
SELECT id FROM t_mem WHERE mapContainsKeyValueLike(m, 'svc', 'ap%') ORDER BY id;
SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'svc', 'ap%') ORDER BY id;

SELECT '-- direct read engages for the pair functions (settings pinned) --';
SELECT 'kv exact', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValue(m, 'level', 'error') SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike on', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 3, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike off', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'lev%', '%rror%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 0, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'kvLike short', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE mapContainsKeyValueLike(m, 'l%', 'e%') SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 10, query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';

DROP TABLE t_mem;
DROP TABLE t_idx;

-- ============================================================================
-- Special-byte keys: m['key'] must probe the index with the original key bytes even when the key
-- contains bytes that a subcolumn name carries through serializeText (newline, tab, NUL, backslash,
-- quote). Under optimize_functions_to_subcolumns = 1 the accessor is rewritten to m.key_<serializeText(key)>;
-- the index helper deserializes that suffix back through the map key type. The index answer must match
-- a plain scan on every variant.
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mem VALUES
    (1, map(unhex('0A'), 'v')),      -- newline byte key
    (2, map(unhex('09'), 'v')),      -- tab byte key
    (3, map(unhex('00'), 'v')),      -- NUL byte key
    (4, map('a\\b', 'v')),           -- backslash key
    (5, map('q"x', 'v')),            -- double-quote key
    (6, map('plain', 'v'));          -- ordinary key
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- m[key] = value over special-byte keys: index == plain scan (subcolumn path) --';
SELECT id FROM t_mem WHERE m[unhex('0A')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('0A')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m[unhex('09')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('09')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m[unhex('00')] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m[unhex('00')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m['a\\b'] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m['a\\b'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;
SELECT id FROM t_mem WHERE m['q"x'] = 'v' ORDER BY id;
SELECT id FROM t_idx WHERE m['q"x'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- startsWith over a special-byte key (subcolumn path) --';
SELECT id FROM t_mem WHERE startsWith(m[unhex('0A')], 'v') ORDER BY id;
SELECT id FROM t_idx WHERE startsWith(m[unhex('0A')], 'v') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- an absent special-byte key matches nothing --';
SELECT count() FROM t_idx WHERE m[unhex('0B')] = 'v' SETTINGS optimize_functions_to_subcolumns = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_mem;
DROP TABLE t_idx;

-- ============================================================================
-- Duplicate-key m['key'] equality/inequality (and their NOT forms): m['key'] returns the first value
-- for the key, while the index stores every (key, value) pair. This pins the behavior of both engines
-- (Memory scan vs keyValuePairs index) so any change to how the index answers these predicates is visible.
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mem VALUES (1, map('k', 'a', 'k', 'b'));
INSERT INTO t_idx VALUES (1, map('k', 'a', 'k', 'b'));

SELECT '======== Memory (no index) ========';
SELECT 'SECOND ARG';
SELECT '=============1';
SELECT * FROM t_mem WHERE m['k'] = 'b';
SELECT '=============2';
SELECT * FROM t_mem WHERE NOT m['k'] != 'b';
SELECT '=============3';
SELECT * FROM t_mem WHERE m['k'] != 'b';
SELECT '=============4';
SELECT * FROM t_mem WHERE NOT m['k'] = 'b';
SELECT 'FIRST ARG';
SELECT '=============1';
SELECT * FROM t_mem WHERE m['k'] = 'a';
SELECT '=============2';
SELECT * FROM t_mem WHERE NOT m['k'] != 'a';
SELECT '=============3';
SELECT * FROM t_mem WHERE m['k'] != 'a';
SELECT '=============4';
SELECT * FROM t_mem WHERE NOT m['k'] = 'a';

SELECT '======== MergeTree + keyValuePairs index ========';
SELECT 'SECOND ARG';
SELECT '=============1';
SELECT * FROM t_idx WHERE m['k'] = 'b';
SELECT '=============2';
SELECT * FROM t_idx WHERE NOT m['k'] != 'b';
SELECT '=============3';
SELECT * FROM t_idx WHERE m['k'] != 'b';
SELECT '=============4';
SELECT * FROM t_idx WHERE NOT m['k'] = 'b';
SELECT 'FIRST ARG';
SELECT '=============1';
SELECT * FROM t_idx WHERE m['k'] = 'a';
SELECT '=============2';
SELECT * FROM t_idx WHERE NOT m['k'] != 'a';
SELECT '=============3';
SELECT * FROM t_idx WHERE m['k'] != 'a';
SELECT '=============4';
SELECT * FROM t_idx WHERE NOT m['k'] = 'a';

DROP TABLE t_mem;
DROP TABLE t_idx;

-- ============================================================================
-- m['key'] IN (v1, ..., vn) over a keyValuePairs index is the union of the exact first-value lookups
-- m['key'] = vi. The index prunes granules and, for literal sets, is used for exact direct read. Results
-- must equal a plain scan. A set with the empty string falls back to a scan (m['key'] = '' is true for
-- rows lacking the key). (The non-materialized-part regression is in 04728_map_kv_tokenizer_bugfixes.)
-- ============================================================================
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;
CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mem VALUES
    (1, map('lvl', 'err')), (2, map('lvl', 'info')), (3, map('lvl', 'warn')),
    (4, map('svc', 'api')), (5, map('lvl', 'debug')), (6, map('lvl', 'err')),
    (7, map('k', 'a', 'k', 'b'));    -- duplicate key: first value 'a'
INSERT INTO t_idx SELECT * FROM t_mem;

SELECT '-- index (granule pruning) == plain scan --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', 'warn') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 1;

SELECT '-- direct read IS used for the literal IN set form (1) --';
SELECT 'direct read', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') SETTINGS query_plan_direct_read_from_text_index = 1, optimize_functions_to_subcolumns = 0) WHERE explain LIKE '%__text_index_%';

SELECT '-- direct-read path == plain scan --';
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', 'warn') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 1, use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- absent values match nothing --';
SELECT count() FROM t_idx WHERE m['lvl'] IN ('nope', 'none') SETTINGS use_skip_indexes = 1;

SELECT '-- empty string in the set: rows without the key match --';
SELECT id FROM t_mem WHERE m['lvl'] IN ('err', '') ORDER BY id;
SELECT id FROM t_idx WHERE m['lvl'] IN ('err', '') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

SELECT '-- duplicate key: first-value (arrayElement) semantics, k=(a,b) matches on a, not b --';
SELECT id FROM t_idx WHERE m['k'] IN ('a') ORDER BY id SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_idx WHERE m['k'] IN ('b') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0;

DROP TABLE t_mem;
DROP TABLE t_idx;
