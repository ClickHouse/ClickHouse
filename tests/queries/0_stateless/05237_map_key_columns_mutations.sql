-- Tags: no-random-settings, no-random-merge-tree-settings

-- Mutation surface of the `with_key_columns` Map serialization: ALTER DELETE with a
-- Map predicate and ALTER UPDATE assigning the Map column both work; updating other
-- columns keeps the Map intact; MATERIALIZE COLUMN and TTL mutations don't corrupt
-- the Map; MOVE / ATTACH PARTITION between two with_key_columns tables works.

DROP TABLE IF EXISTS t_mut;
CREATE TABLE t_mut
(
    id UInt32,
    v UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree ORDER BY id PARTITION BY id % 2
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_mut VALUES (1, 10, {'a': 1}), (2, 20, {'b': 2}), (3, 30, {'a': 3, 'c': 33});

-- DELETE with a Map predicate works.
ALTER TABLE t_mut DELETE WHERE mapContains(m, 'c') AND id = 100 SETTINGS mutations_sync = 2;
ALTER TABLE t_mut DELETE WHERE mapContains(m, 'a') AND id = 1 SETTINGS mutations_sync = 2;
SELECT id, v, m FROM t_mut ORDER BY id;

-- UPDATE assigning the Map column works, including keys that are new to the part.
ALTER TABLE t_mut UPDATE m = map('z', 9) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT id, v, m FROM t_mut ORDER BY id;

-- UPDATE with an expression that merges existing keys and new keys.
ALTER TABLE t_mut UPDATE m = mapConcat(m, map('zz', 90)) WHERE id = 3 SETTINGS mutations_sync = 2;
SELECT id, v, m FROM t_mut ORDER BY id;

-- Updating a different column works and the Map survives.
ALTER TABLE t_mut UPDATE v = 99 WHERE id = 3 SETTINGS mutations_sync = 2;
SELECT id, v, m FROM t_mut ORDER BY id;

-- A TTL redelete mutation rewrites parts and must not corrupt the Map.
ALTER TABLE t_mut MODIFY TTL toDateTime('2020-01-01') + toIntervalSecond(toUInt32(1000000000 + v));
ALTER TABLE t_mut MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT id, v, m, mapContains(m, 'a'), mapContains(m, 'z') FROM t_mut ORDER BY id;

-- ATTACH PARTITION FROM between two with_key_columns tables.
DROP TABLE IF EXISTS t_mut_dst;
CREATE TABLE t_mut_dst AS t_mut
ENGINE = MergeTree ORDER BY id PARTITION BY id % 2
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_mut_dst VALUES (10, 100, {'d': 4}), (11, 110, {'e': 5});

ALTER TABLE t_mut MOVE PARTITION 1 TO TABLE t_mut_dst;
SELECT 'dst after move';
SELECT id, v, m FROM t_mut_dst ORDER BY id;
SELECT 'src after move';
SELECT id, v, m FROM t_mut ORDER BY id;

ALTER TABLE t_mut_dst ATTACH PARTITION 0 FROM t_mut;
SELECT 'dst after attach';
SELECT id, v, m FROM t_mut_dst ORDER BY id;

DROP TABLE t_mut;
DROP TABLE t_mut_dst;
