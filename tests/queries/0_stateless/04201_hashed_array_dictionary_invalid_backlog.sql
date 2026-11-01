-- Test: exercises BAD_ARGUMENTS validation for `SHARD_LOAD_QUEUE_BACKLOG = 0` in `HASHED_ARRAY` and `COMPLEX_KEY_HASHED_ARRAY` layouts
-- Covers: src/Dictionaries/HashedArrayDictionary.cpp:1261 — `if (shard_load_queue_backlog <= 0)` branch added by PR #60926
-- Before this PR, `HASHED_ARRAY` did not parse `shard_load_queue_backlog` from config at all (silently used default 10000).
-- The PR makes `HASHED_ARRAY` read the parameter and validate it — this validation has zero existing test coverage.

DROP DICTIONARY IF EXISTS dict_hashed_array_invalid_backlog;
DROP DICTIONARY IF EXISTS dict_complex_hashed_array_invalid_backlog;
DROP DICTIONARY IF EXISTS dict_hashed_array_valid_backlog;
DROP TABLE IF EXISTS source_simple;
DROP TABLE IF EXISTS source_complex;

CREATE TABLE source_simple (id UInt64, value String) ENGINE = TinyLog;
INSERT INTO source_simple VALUES (1, 'a'), (2, 'b');

CREATE TABLE source_complex (key1 UInt64, key2 String, value String) ENGINE = TinyLog;
INSERT INTO source_complex VALUES (1, 'k', 'a'), (2, 'k', 'b');

-- HASHED_ARRAY rejects SHARD_LOAD_QUEUE_BACKLOG = 0.
-- Validation in registerDictionaryArrayHashed runs at dictionary *load* time, not at CREATE time
-- (dictionaries are lazily loaded by default, so CREATE DICTIONARY itself returns OK).
-- SYSTEM RELOAD DICTIONARY forces the load synchronously so the assertion is pinned to the
-- validation step rather than to the dictGet runtime path.
CREATE DICTIONARY dict_hashed_array_invalid_backlog
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_simple'))
LAYOUT(HASHED_ARRAY(SHARD_LOAD_QUEUE_BACKLOG 0))
LIFETIME(MIN 1 MAX 1000);

SYSTEM RELOAD DICTIONARY dict_hashed_array_invalid_backlog; -- { serverError BAD_ARGUMENTS }

-- COMPLEX_KEY_HASHED_ARRAY shares the same registration code path (registerDictionaryArrayHashed) — same validation
CREATE DICTIONARY dict_complex_hashed_array_invalid_backlog
(
    key1 UInt64,
    key2 String,
    value String
)
PRIMARY KEY key1, key2
SOURCE(CLICKHOUSE(TABLE 'source_complex'))
LAYOUT(COMPLEX_KEY_HASHED_ARRAY(SHARD_LOAD_QUEUE_BACKLOG 0))
LIFETIME(MIN 1 MAX 1000);

SYSTEM RELOAD DICTIONARY dict_complex_hashed_array_invalid_backlog; -- { serverError BAD_ARGUMENTS }

-- Sanity check: a positive value works
CREATE DICTIONARY dict_hashed_array_valid_backlog
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_simple'))
LAYOUT(HASHED_ARRAY(SHARDS 2 SHARD_LOAD_QUEUE_BACKLOG 5))
LIFETIME(MIN 1 MAX 1000);

SELECT dictGet('dict_hashed_array_valid_backlog', 'value', toUInt64(1));
SELECT dictGet('dict_hashed_array_valid_backlog', 'value', toUInt64(2));

DROP DICTIONARY IF EXISTS dict_hashed_array_invalid_backlog;
DROP DICTIONARY IF EXISTS dict_complex_hashed_array_invalid_backlog;
DROP DICTIONARY IF EXISTS dict_hashed_array_valid_backlog;
DROP TABLE source_simple;
DROP TABLE source_complex;
