-- Tags: no-random-settings, no-random-merge-tree-settings

-- Compact parts with the `with_key_columns` Map serialization: inserts, point and
-- whole-map reads, merges of compact parts and compact→wide crossing, mutations, and
-- multi-granule parts. Small parts are Compact by default, so this is the common path.

DROP TABLE IF EXISTS t_compact_map;
CREATE TABLE t_compact_map (id UInt32, m Map(String, Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

-- Default thresholds produce Compact parts for such small inserts.
INSERT INTO t_compact_map VALUES (1, {'a': 1, 'b': 2}), (2, {'a': 10, 'c': 30});
INSERT INTO t_compact_map VALUES (3, {'b': 200, 'd': NULL});

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_map' AND active ORDER BY name;
SELECT id, m FROM t_compact_map ORDER BY id;
SELECT id, m['a'], m['zz'] FROM t_compact_map ORDER BY id;
SELECT id, mapKeys(m), mapContains(m, 'd') FROM t_compact_map ORDER BY id;

-- Merge of two compact parts stays compact.
OPTIMIZE TABLE t_compact_map FINAL;
SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_map' AND active;
SELECT id, m FROM t_compact_map ORDER BY id;

DROP TABLE t_compact_map;

-- Multi-granule compact part: keys appear in different granules, per-granule reads must
-- seek each key's substream to the granule's mark.
DROP TABLE IF EXISTS t_compact_granules;
CREATE TABLE t_compact_granules (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', index_granularity = 2;

INSERT INTO t_compact_granules VALUES (1, {'a': 1}), (2, {'b': 2}), (3, {'a': 3, 'c': 33}), (4, {'d': 4}), (5, {'e': 5});
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_granules' AND active;
SELECT id, m FROM t_compact_granules ORDER BY id;
SELECT id, m['a'], m['c'], m['e'] FROM t_compact_granules ORDER BY id;
SELECT sum(m['a']), sum(m['b']), sum(m['c']), sum(m['d']), sum(m['e']) FROM t_compact_granules;

DROP TABLE t_compact_granules;

-- Merge crossing the wide threshold: compact source parts merge into a wide part, and
-- the merged data keeps every key.
DROP TABLE IF EXISTS t_compact_to_wide;
CREATE TABLE t_compact_to_wide (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         min_bytes_for_wide_part = 100, min_rows_for_wide_part = 0;

INSERT INTO t_compact_to_wide VALUES (1, {'a': 1}), (2, {'b': 2});
INSERT INTO t_compact_to_wide VALUES (3, {'a': 10, 'c': 30}), (4, {'b': 20, 'd': 40});
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active ORDER BY name;
OPTIMIZE TABLE t_compact_to_wide FINAL;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact_to_wide' AND active;
SELECT id, m FROM t_compact_to_wide ORDER BY id;

DROP TABLE t_compact_to_wide;

-- Mutations on a compact part.
DROP TABLE IF EXISTS t_compact_mutate;
CREATE TABLE t_compact_mutate (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_compact_mutate VALUES (1, {'a': 1}), (2, {'b': 2});
ALTER TABLE t_compact_mutate UPDATE m = map('z', 99) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT id, m FROM t_compact_mutate ORDER BY id;
ALTER TABLE t_compact_mutate DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT id, m FROM t_compact_mutate ORDER BY id;

DROP TABLE t_compact_mutate;

-- A key absent from a compact part reads as the value type's default from that part,
-- and merging a part without the key with parts that have it keeps presence = 0.
DROP TABLE IF EXISTS t_compact_disjoint;
CREATE TABLE t_compact_disjoint (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_compact_disjoint VALUES (1, {'a': 1});
INSERT INTO t_compact_disjoint VALUES (2, {'b': 2});
SELECT id, m['a'], m['b'] FROM t_compact_disjoint ORDER BY id;
OPTIMIZE TABLE t_compact_disjoint FINAL;
SELECT id, m, m['a'], m['b'] FROM t_compact_disjoint ORDER BY id;

DROP TABLE t_compact_disjoint;

-- Empty Maps and an ALTER-added Map column: the empty part's manifest has no keys and
-- no compressed block at all; the added column's default-derived keys are picked up by
-- the merge.
DROP TABLE IF EXISTS t_compact_empty;
CREATE TABLE t_compact_empty (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';

INSERT INTO t_compact_empty VALUES (1, map());
INSERT INTO t_compact_empty VALUES (2, {'a': 1});
SELECT id, m, m['a'] FROM t_compact_empty ORDER BY id;
OPTIMIZE TABLE t_compact_empty FINAL;
SELECT id, m FROM t_compact_empty ORDER BY id;

ALTER TABLE t_compact_empty ADD COLUMN m2 Map(String, UInt64) DEFAULT map('d', id);
INSERT INTO t_compact_empty (id, m) VALUES (3, {'e': 5});
SELECT id, m2, m2['d'] FROM t_compact_empty ORDER BY id;
OPTIMIZE TABLE t_compact_empty FINAL;
SELECT id, m, m2 FROM t_compact_empty ORDER BY id;

DROP TABLE t_compact_empty;
