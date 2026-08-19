-- The keyValuePairs text-index tokenizer stores each map (key, value) pair as one token
-- (key || value || reversed-varint of the key length). This first version accelerates exact predicates
-- over Map(String, String) / Map(LowCardinality(String), LowCardinality(String)) via granule pruning:
--   m['key'] = 'value'   and   m['key'] IN ('v1', ...)
-- The predicate is re-checked on surviving granules (no direct read yet), so results must equal a plain
-- scan whether or not the index is used.

-- Verify the tokenizer is registered.
SELECT name FROM system.tokenizers WHERE name = 'keyValuePairs';

DROP TABLE IF EXISTS t_kv;
CREATE TABLE t_kv
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO t_kv VALUES (1, {'foo':'bar'}), (2, {'foo':'baz','k2':'v2'}), (3, {'lvl':'err'}), (4, {'lvl':'warn'});

-- Each distinct pair is one token: namespace byte (1 = first occurrence) || key || value || chr(length(key)).
SELECT hex(token), cardinality FROM mergeTreeTextIndex(currentDatabase(), t_kv, idx) ORDER BY token;
-- The 'foo' -> 'bar' token: first-occurrence namespace 1, key 'foo' (length 3), so char(1) || 'foo' || 'bar' || char(3).
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), t_kv, idx) WHERE token = concat(char(1), 'foo', 'bar', char(3));

-- Exact key-value lookups answered by the index (granule pruning).
SELECT id FROM t_kv WHERE m['foo'] = 'bar' ORDER BY id;
SELECT id FROM t_kv WHERE m['foo'] = 'baz' ORDER BY id;
SELECT id FROM t_kv WHERE m['k2'] = 'v2' ORDER BY id;
-- No false match across pairs; unknown key/value matches nothing.
SELECT id FROM t_kv WHERE m['foo'] = 'v2' ORDER BY id;
SELECT id FROM t_kv WHERE m['nope'] = 'bar' ORDER BY id;

-- The text index is used for the equality (and for the subcolumn form).
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_kv WHERE m['foo'] = 'bar') WHERE explain LIKE '%Name: idx%';
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_kv WHERE m['foo'] = 'bar' SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain LIKE '%Name: idx%';

-- m['key'] IN (...) is the union of exact m['key'] = vi lookups.
SELECT id FROM t_kv WHERE m['lvl'] IN ('err', 'warn') ORDER BY id;
SELECT id FROM t_kv WHERE m['lvl'] IN ('none', 'gone') ORDER BY id;
SELECT extract(explain, 'Name: idx') FROM (EXPLAIN indexes = 1 SELECT id FROM t_kv WHERE m['lvl'] IN ('err', 'warn')) WHERE explain LIKE '%Name: idx%';

-- Map default-value semantics: m['absent'] is '', so a predicate true on '' must still match rows lacking
-- the key (which have no token). Such predicates fall back to a full scan.
DROP TABLE IF EXISTS t_kv_def;
CREATE TABLE t_kv_def
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_kv_def VALUES (1, {'foo':'bar'}), (2, {'x':'y'}), (3, {'foo':''});
SELECT id FROM t_kv_def WHERE m['foo'] = '' ORDER BY id;
SELECT id FROM t_kv_def WHERE m['lvl'] IN ('err', '') ORDER BY id;
SELECT id FROM t_kv_def WHERE m['foo'] = 'bar' ORDER BY id;
DROP TABLE t_kv_def;

-- LowCardinality(String) keys and values are supported.
DROP TABLE IF EXISTS t_kv_lc;
CREATE TABLE t_kv_lc
(
    id UInt64,
    m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_kv_lc VALUES (1, {'level':'error'}), (2, {'level':'info','svc':'api'});
SELECT id FROM t_kv_lc WHERE m['level'] = 'error' ORDER BY id;
SELECT id FROM t_kv_lc WHERE m['level'] IN ('error', 'info') ORDER BY id;
DROP TABLE t_kv_lc;

-- Duplicate keys: m['key'] returns the first value; the index pins the first occurrence (is_rest = 0).
DROP TABLE IF EXISTS t_kv_dup;
CREATE TABLE t_kv_dup
(
    id UInt64,
    m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_kv_dup VALUES (1, map('k', 'a', 'k', 'b'));
SELECT id FROM t_kv_dup WHERE m['k'] = 'a' ORDER BY id;
SELECT count() FROM t_kv_dup WHERE m['k'] = 'b';
DROP TABLE t_kv_dup;

-- Long keys exercise the multi-byte token trailer (key >= 64 bytes takes the reversed-varint path);
-- exact lookups must still match. 127 = last single-trailer-byte key, 128 = first multi-byte trailer.
DROP TABLE IF EXISTS t_kv_long;
CREATE TABLE t_kv_long (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_kv_long VALUES
    (1, map(repeat('k', 127), 'v1')),
    (2, map(repeat('K', 128), 'v2')),
    (3, map(repeat('x', 200), repeat('y', 300)));
SELECT id FROM t_kv_long WHERE m[repeat('k', 127)] = 'v1' ORDER BY id;
SELECT id FROM t_kv_long WHERE m[repeat('K', 128)] = 'v2' ORDER BY id;
SELECT id FROM t_kv_long WHERE m[repeat('x', 200)] = repeat('y', 300) ORDER BY id;
SELECT id FROM t_kv_long WHERE m[repeat('K', 128)] = 'nope' ORDER BY id;
DROP TABLE t_kv_long;

-- Special-byte keys (newline, tab, NUL, backslash, quote): m['key'] must probe with the original bytes,
-- including through the subcolumn rewrite (m.key_<serializeText(key)>).
DROP TABLE IF EXISTS t_kv_bytes;
CREATE TABLE t_kv_bytes (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_kv_bytes VALUES
    (1, map(unhex('0A'), 'v')), (2, map(unhex('09'), 'v')), (3, map(unhex('00'), 'v')),
    (4, map('a\\b', 'v')), (5, map('q"x', 'v')), (6, map('plain', 'v'));
SELECT id FROM t_kv_bytes WHERE m[unhex('0A')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id FROM t_kv_bytes WHERE m[unhex('00')] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id FROM t_kv_bytes WHERE m['a\\b'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id FROM t_kv_bytes WHERE m['q"x'] = 'v' ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_kv_bytes WHERE m[unhex('0B')] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_kv_bytes;

DROP TABLE t_kv;

-- The keyValuePairs tokenizer requires a Map with String or LowCardinality(String) keys/values.
CREATE TABLE t_kv_bad1 (id UInt64, m Map(String, UInt64), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- The keyValuePairs tokenizer requires a Map column.
CREATE TABLE t_kv_bad2 (id UInt64, s String, INDEX idx s TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- FixedString keys/values are rejected (query-side key is unpadded while the index stores padded bytes).
CREATE TABLE t_kv_bad3 (id UInt64, m Map(FixedString(3), String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
-- The keyValuePairs tokenizer does not support the preprocessor / postprocessor options.
CREATE TABLE t_kv_bad4 (id UInt64, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', preprocessor = 'toString(m)')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_kv_bad5 (id UInt64, m Map(String, String), INDEX idx m TYPE text(tokenizer = 'keyValuePairs', postprocessor = 'lower(token)')) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
