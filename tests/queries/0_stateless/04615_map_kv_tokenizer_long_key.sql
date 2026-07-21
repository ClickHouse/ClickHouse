-- Covers the keyValuePairs token trailer for keys at and beyond the single-byte length boundary.
-- The trailer encodes (length(key) << 1) | is_rest, so a key shorter than 64 bytes fits one trailer
-- byte; a key of 64 bytes or more takes the multi-byte reversed-varint path in encodeMapKeyValueToken /
-- decodeMapKeyValueToken (127/128/200-byte keys below all exercise it). The index must answer exactly
-- like a plain scan for all key/value lengths.

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
