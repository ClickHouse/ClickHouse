-- Tags: no-random-settings, no-random-merge-tree-settings

-- DDL surface of the `with_key_columns` Map serialization: the key type must be
-- exactly String and the zero-level override must stay at its default. Maps in
-- table keys, skip indices, projections, nested Maps and Compact parts all work.
-- Value types are free (Nullable / Array / LowCardinality values work).

-- Bad key types: only exactly String is allowed.
CREATE TABLE t_bad_key_fs (id UInt32, m Map(FixedString(4), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_bad_key_nullable (id UInt32, m Map(Nullable(String), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_bad_key_lc (id UInt32, m Map(LowCardinality(String), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_bad_key_uint (id UInt32, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

-- Nested Maps (inside Array / Tuple) are rejected: the per-key state prefix of a nested
-- Map shares its `keys` stream with per-element granule data, which the prefix reader
-- cannot delimit.
CREATE TABLE t_nested_array (id UInt32, m Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_nested_tuple (id UInt32, m Tuple(a Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

-- Map in the sorting / primary / partition / sampling key works.
DROP TABLE IF EXISTS t_key_order;
CREATE TABLE t_key_order (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY (id, m)
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_key_order VALUES (1, {'a': 1}), (1, {'b': 2}), (2, {'a': 10, 'c': 30});
SELECT id, m FROM t_key_order ORDER BY id, m;
OPTIMIZE TABLE t_key_order FINAL;
SELECT id, m FROM t_key_order ORDER BY id, m;
DROP TABLE t_key_order;

DROP TABLE IF EXISTS t_key_primary;
CREATE TABLE t_key_primary (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY (id, m) PRIMARY KEY (id, m)
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_key_primary VALUES (1, {'a': 1}), (2, {'b': 2});
SELECT id, m FROM t_key_primary ORDER BY id, m;
DROP TABLE t_key_primary;

DROP TABLE IF EXISTS t_key_partition;
CREATE TABLE t_key_partition (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id PARTITION BY length(mapKeys(m))
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_key_partition VALUES (1, {'a': 1}), (2, {'a': 1, 'b': 2}), (3, map());
SELECT id, m FROM t_key_partition ORDER BY id;
SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_key_partition' AND active ORDER BY partition;
DROP TABLE t_key_partition;

-- Projections work on tables with with_key_columns Maps.
DROP TABLE IF EXISTS t_projection;
CREATE TABLE t_projection (id UInt32, m Map(String, UInt64), PROJECTION p (SELECT id, m['a'] ORDER BY id))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_projection VALUES (1, {'a': 1, 'b': 2}), (2, {'b': 3}), (3, {'a': 10});
SELECT id, m FROM t_projection ORDER BY id;
SELECT id, m['a'] FROM t_projection ORDER BY id;
OPTIMIZE TABLE t_projection FINAL;
SELECT id, m FROM t_projection ORDER BY id;
ALTER TABLE t_projection MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT id, m['a'] FROM t_projection ORDER BY id;
DROP TABLE t_projection;

-- Skip indices on the Map column work.
DROP TABLE IF EXISTS t_skip_index;
CREATE TABLE t_skip_index (id UInt32, m Map(String, UInt64), INDEX idx m TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_skip_index VALUES (1, {'a': 1}), (2, {'b': 2}), (3, {'a': 3, 'c': 33});
SELECT id FROM t_skip_index WHERE mapContains(m, 'a') ORDER BY id;
DROP TABLE t_skip_index;

-- Compact parts work: the per-key streams of a `with_key_columns` Map are seeded with
-- the part-level key set before the first mark is recorded, and each key's substreams get
-- their own compressed block per granule inside `data.bin`.
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (id UInt32, m Map(String, Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         min_bytes_for_wide_part = 1e10, min_rows_for_wide_part = 1e10;
INSERT INTO t_compact VALUES (1, {'a': 1});
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
SELECT id, m FROM t_compact ORDER BY id;
DROP TABLE t_compact;

-- The zero-level override is ignored for with_key_columns (it only applies to with_buckets);
-- setting it does not break DDL and zero-level parts still use the per-key layout.
DROP TABLE IF EXISTS t_zero_level_ignored;
CREATE TABLE t_zero_level_ignored (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_buckets';
INSERT INTO t_zero_level_ignored VALUES (1, {'a': 1});
SELECT id, m, m['a'] FROM t_zero_level_ignored ORDER BY id;
DROP TABLE t_zero_level_ignored;

-- Value types are free.
DROP TABLE IF EXISTS t_value_nullable;
CREATE TABLE t_value_nullable (id UInt32, m Map(String, Nullable(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_value_nullable VALUES (1, {'a': 'x', 'b': NULL});
SELECT m FROM t_value_nullable;
DROP TABLE t_value_nullable;

DROP TABLE IF EXISTS t_value_array;
CREATE TABLE t_value_array (id UInt32, m Map(String, Array(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_value_array VALUES (1, {'a': [1, 2], 'b': []});
SELECT m FROM t_value_array;
DROP TABLE t_value_array;

DROP TABLE IF EXISTS t_value_lc;
CREATE TABLE t_value_lc (id UInt32, m Map(String, LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_value_lc VALUES (1, {'a': 'x', 'b': 'y'});
SELECT m FROM t_value_lc;
DROP TABLE t_value_lc;

-- OPTIMIZE DEDUPLICATE works.
DROP TABLE IF EXISTS t_dedup;
CREATE TABLE t_dedup (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_dedup VALUES (1, {'a': 1}), (1, {'a': 1}), (2, {'b': 2});
OPTIMIZE TABLE t_dedup DEDUPLICATE;
SELECT id, m FROM t_dedup ORDER BY id;
DROP TABLE t_dedup;

-- Flipping the serialization version with existing data parts is rejected.
DROP TABLE IF EXISTS t_flip;
CREATE TABLE t_flip (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
INSERT INTO t_flip VALUES (1, {'a': 1});
-- Flipping the setting with existing data parts is rejected.
ALTER TABLE t_flip MODIFY SETTING map_serialization_version = 'basic'; -- { serverError SUPPORT_IS_DISABLED }
-- Flipping an empty table is fine.
DROP TABLE IF EXISTS t_flip_empty;
CREATE TABLE t_flip_empty (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns';
ALTER TABLE t_flip_empty MODIFY SETTING map_serialization_version = 'basic';
SELECT value FROM system.merge_tree_settings WHERE 0; -- keep client in sync
DROP TABLE t_flip_empty;
DROP TABLE t_flip;
