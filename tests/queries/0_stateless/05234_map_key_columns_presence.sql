-- Tags: no-random-settings, no-random-merge-tree-settings

-- Presence vs value-NULL matrix for the `with_key_columns` Map serialization:
--   - non-nullable V: absent key reads as the type default (0)
--   - Nullable V: absent key reads as NULL with mapContains = 0
--   - Nullable V: present NULL reads as NULL with mapContains = 1
-- mapKeys / mapValues / mapContains stay consistent with presence.

DROP TABLE IF EXISTS t_presence_plain;
CREATE TABLE t_presence_plain
(
    id UInt32,
    m Map(String, UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_presence_plain VALUES
    (1, {'a': 1, 'b': 2}),   -- a and b present
    (2, {'a': 10}),          -- only a present
    (3, {'b': 20, 'c': 30}), -- a absent
    (4, {});                 -- nothing present

-- Absent key of non-nullable V reads as the default 0.
SELECT id, m['a'], m['b'], m['c'], m['zzz'] FROM t_presence_plain ORDER BY id;
SELECT id, mapContains(m, 'a'), mapContains(m, 'b'), mapContains(m, 'c'), mapContains(m, 'zzz') FROM t_presence_plain ORDER BY id;
SELECT id, mapKeys(m), mapValues(m) FROM t_presence_plain ORDER BY id;
SELECT id, m, mapContains(m, 'a') + mapContains(m, 'b') + mapContains(m, 'c') = length(mapKeys(m)) FROM t_presence_plain ORDER BY id;
-- mapValues must line up with mapKeys.
SELECT id, arrayAll((k, v) -> m[k] = v, mapKeys(m), mapValues(m)) FROM t_presence_plain ORDER BY id;
DROP TABLE t_presence_plain;

DROP TABLE IF EXISTS t_presence_nullable;
CREATE TABLE t_presence_nullable
(
    id UInt32,
    m Map(String, Nullable(UInt64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_presence_nullable VALUES
    (1, {'a': 1, 'b': NULL}),   -- a present with value, b present NULL
    (2, {'a': NULL}),           -- a present NULL, b absent
    (3, {'b': 20}),             -- a absent, b present with value
    (4, {});                    -- both absent

-- Absent reads NULL with mapContains = 0; present NULL reads NULL with mapContains = 1.
SELECT id, m['a'], mapContains(m, 'a'), m['b'], mapContains(m, 'b') FROM t_presence_nullable ORDER BY id;
-- isNull alone cannot distinguish present NULL from absent; mapContains can.
SELECT id, isNull(m['a']), mapContains(m, 'a') FROM t_presence_nullable ORDER BY id;
SELECT id, mapKeys(m), mapValues(m) FROM t_presence_nullable ORDER BY id;
SELECT id, arrayAll((k, v) -> (m[k] = v) OR (isNull(m[k]) AND isNull(v)), mapKeys(m), mapValues(m)) FROM t_presence_nullable ORDER BY id;
DROP TABLE t_presence_nullable;
