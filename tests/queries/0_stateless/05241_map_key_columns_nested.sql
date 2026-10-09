-- Tags: no-random-settings, no-random-merge-tree-settings

-- Maps nested inside Array/Tuple with the `with_key_columns` Map serialization.
-- The `m.keys` manifest is self-delimiting, so a nested Map's manifest can be read
-- exactly even though the outer serialization keeps writing granule data into the
-- same nested stream. Point lookups, missing keys, duplicate/empty key rejection,
-- merging of parts with disjoint key sets, and mutations are covered, in both
-- Compact and Wide parts.

-- Wide parts: arrays of varying length, keys varying per element, the same key in
-- different elements of one row with different values, empty maps and empty arrays.
DROP TABLE IF EXISTS t_arr_wide;
CREATE TABLE t_arr_wide (id UInt32, arr Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_arr_wide VALUES (1, [{'a': 1, 'b': 2}, {'a': 10, 'c': 30}]);
INSERT INTO t_arr_wide SELECT 2, [];
INSERT INTO t_arr_wide SELECT 3, [mapApply((k, v) -> (k, v), map())];
INSERT INTO t_arr_wide SELECT 4, [map('b', 7), mapApply((k, v) -> (k, v), map()), map('d', 4, 'e', 5)];
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_arr_wide' AND active ORDER BY name;

-- Point lookups and key functions per element.
SELECT id, arr[1]['a'], arr[2]['a'] FROM t_arr_wide WHERE length(arr) >= 2 ORDER BY id;
SELECT id, mapContains(arr[2], 'c'), mapContains(arr[2], 'a') FROM t_arr_wide WHERE length(arr) >= 2 ORDER BY id;
SELECT id, mapKeys(arr[1]) FROM t_arr_wide WHERE length(arr) >= 1 ORDER BY id;

-- Missing key reads as the default value of the value type.
SELECT id, arr[1]['zzz'], mapContains(arr[1], 'zzz') FROM t_arr_wide WHERE length(arr) >= 1 ORDER BY id;

-- Whole-column round trip.
SELECT id, arr FROM t_arr_wide ORDER BY id;

-- Merging parts with disjoint nested key sets: the merged part has the union of the
-- key sets and presence stays correct per element.
OPTIMIZE TABLE t_arr_wide FINAL;
SELECT id, arr FROM t_arr_wide ORDER BY id;
SELECT id, arr[3]['d'], mapContains(arr[3], 'b') FROM t_arr_wide WHERE id = 4;
SELECT id, arr[2]['a'], mapContains(arr[2], 'a') FROM t_arr_wide WHERE length(arr) >= 2 ORDER BY id;
DROP TABLE t_arr_wide;

-- Compact parts: the manifest and the per-element rows share the nested stream in
-- `data.bin`; the self-delimiting manifest must not over-read.
DROP TABLE IF EXISTS t_arr_compact;
CREATE TABLE t_arr_compact (id UInt32, arr Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         min_bytes_for_wide_part = 1e10, min_rows_for_wide_part = 1e10;
INSERT INTO t_arr_compact VALUES (1, [{'a': 1, 'b': 2}, {'a': 10, 'c': 30}]);
INSERT INTO t_arr_compact SELECT 2, [];
INSERT INTO t_arr_compact SELECT 3, [mapApply((k, v) -> (k, v), map())];
INSERT INTO t_arr_compact SELECT 4, [map('b', 7), mapApply((k, v) -> (k, v), map()), map('d', 4, 'e', 5)];
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_arr_compact' AND active ORDER BY name;
SELECT id, arr FROM t_arr_compact ORDER BY id;
SELECT id, arr[1]['a'], arr[2]['a'] FROM t_arr_compact WHERE length(arr) >= 2 ORDER BY id;
SELECT id, arr[1]['zzz'], mapContains(arr[1], 'zzz') FROM t_arr_compact WHERE length(arr) >= 1 ORDER BY id;
OPTIMIZE TABLE t_arr_compact FINAL;
SELECT id, arr FROM t_arr_compact ORDER BY id;
SELECT id, arr[3]['d'], mapContains(arr[3], 'b') FROM t_arr_compact WHERE id = 4;
DROP TABLE t_arr_compact;

-- Compact parts with more than one granule: granule data follows the manifest in the
-- nested stream, so an over-reading manifest would hit the next granule's data.
DROP TABLE IF EXISTS t_arr_granules;
CREATE TABLE t_arr_granules (id UInt32, arr Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         min_bytes_for_wide_part = 1e10, min_rows_for_wide_part = 1e10,
         index_granularity = 2;
INSERT INTO t_arr_granules
SELECT number, [map(concat('k', toString(number % 3)), number), map('a', number + 100)]
FROM numbers(8);
SELECT id, arr FROM t_arr_granules ORDER BY id;
SELECT id, arr[1]['k1'], arr[2]['a'] FROM t_arr_granules ORDER BY id;
-- Reading from the middle of the part: the manifest of a later granule must be
-- found at that granule's mark, without reading earlier granules.
SELECT id, arr FROM t_arr_granules WHERE id >= 6 ORDER BY id;
SELECT id, arr FROM t_arr_granules WHERE id = 7;
DROP TABLE t_arr_granules;

-- Tuple with a Map element.
DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (id UInt32, t Tuple(m Map(String, UInt64), x UInt8))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_tuple VALUES (1, ({'a': 1}, 10)), (3, ({'a': 3, 'z': 26}, 30));
INSERT INTO t_tuple SELECT 2, tuple(mapApply((k, v) -> (k, v), map()), 20);
SELECT id, t FROM t_tuple ORDER BY id;
SELECT id, t.m['a'], t.m['z'], t.x FROM t_tuple ORDER BY id;
OPTIMIZE TABLE t_tuple FINAL;
SELECT id, t FROM t_tuple ORDER BY id;
DROP TABLE t_tuple;

-- Array of Tuples containing Maps, mixing both nesting kinds.
DROP TABLE IF EXISTS t_arr_tuple;
CREATE TABLE t_arr_tuple (id UInt32, at Array(Tuple(m Map(String, UInt64), x UInt8)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_arr_tuple VALUES (1, [({'a': 1}, 1), ({'b': 2}, 2)]), (2, []);
SELECT id, at FROM t_arr_tuple ORDER BY id;
SELECT id, at[1].m['a'], at[2].m['b'] FROM t_arr_tuple WHERE id = 1;
DROP TABLE t_arr_tuple;

-- Presence vs NULL: a Nullable value can be present-but-NULL or absent.
DROP TABLE IF EXISTS t_arr_nullable;
CREATE TABLE t_arr_nullable (id UInt32, arr Array(Map(String, Nullable(UInt64))))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_arr_nullable VALUES (1, [{'a': NULL}]), (2, [{'a': 5}]);
INSERT INTO t_arr_nullable SELECT 1, [map('a', NULL), mapApply((k, v) -> (k, v), map())];
SELECT id, arr FROM t_arr_nullable ORDER BY id;
SELECT id, arr[1]['a'], mapContains(arr[1], 'a'), mapContains(arr[1], 'zzz') FROM t_arr_nullable ORDER BY id;
DROP TABLE t_arr_nullable;

-- Duplicate keys within ONE map element are rejected; the same key in DIFFERENT
-- elements of one row is fine.
DROP TABLE IF EXISTS t_arr_dup;
CREATE TABLE t_arr_dup (id UInt32, arr Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_arr_dup VALUES (1, [{'a': 1, 'a': 2}]); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_arr_dup VALUES (1, [{'a': 1}, {'a': 2}]);
INSERT INTO t_arr_dup VALUES (2, [{'': 1}]); -- { serverError BAD_ARGUMENTS }
INSERT INTO t_arr_dup SELECT 2, [mapApply((k, v) -> (k, v), map()), map('', 1)]; -- { serverError BAD_ARGUMENTS }
SELECT id, arr FROM t_arr_dup ORDER BY id;
DROP TABLE t_arr_dup;

-- Mutations touching the nested Map column.
DROP TABLE IF EXISTS t_arr_mutate;
CREATE TABLE t_arr_mutate (id UInt32, arr Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_arr_mutate VALUES (1, [{'a': 1}]), (2, [{'b': 2}]);
INSERT INTO t_arr_mutate SELECT 3, [mapApply((k, v) -> (k, v), map())];
ALTER TABLE t_arr_mutate UPDATE arr = [map('c', 3)] WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT id, arr FROM t_arr_mutate ORDER BY id;
ALTER TABLE t_arr_mutate DELETE WHERE mapContains(arr[1], 'a') SETTINGS mutations_sync = 2;
SELECT id, arr FROM t_arr_mutate ORDER BY id;
DROP TABLE t_arr_mutate;
