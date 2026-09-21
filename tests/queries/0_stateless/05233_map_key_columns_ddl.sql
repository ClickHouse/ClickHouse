-- Tags: no-random-settings, no-random-merge-tree-settings

-- DDL surface of the `with_key_columns` Map serialization: key type must be exactly
-- String, nested Maps / keys / projections / skip indices are rejected, the two
-- version settings must agree, and flipping the version with existing data is rejected.
-- Value types are free (Nullable / Array / LowCardinality values work).

-- Bad key types: only exactly String is allowed.
CREATE TABLE t_bad_key_fs (id UInt32, m Map(FixedString(4), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_bad_key_nullable (id UInt32, m Map(Nullable(String), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_bad_key_lc (id UInt32, m Map(LowCardinality(String), UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_bad_key_uint (id UInt32, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

-- Nested Maps are rejected at CREATE time.
CREATE TABLE t_nested_array (id UInt32, m Array(Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_nested_tuple (id UInt32, m Tuple(a Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

-- Map in the sorting / primary / partition / sampling key is rejected.
CREATE TABLE t_key_order (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY (id, m)
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_key_primary (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY (id, m) PRIMARY KEY (id, m)
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_key_partition (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id PARTITION BY m
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_key_sample (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id SAMPLE BY m
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

-- Projections are rejected on tables with with_key_columns Maps.
CREATE TABLE t_projection (id UInt32, m Map(String, UInt64), PROJECTION p (SELECT id ORDER BY id))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

-- Skip indices on the Map column are rejected.
CREATE TABLE t_skip_index (id UInt32, m Map(String, UInt64), INDEX idx m TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

-- The two version settings must agree.
CREATE TABLE t_mismatch (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'basic'; -- { serverError INVALID_SETTING_VALUE }

-- Value types are free.
DROP TABLE IF EXISTS t_value_nullable;
CREATE TABLE t_value_nullable (id UInt32, m Map(String, Nullable(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_value_nullable VALUES (1, {'a': 'x', 'b': NULL});
SELECT m FROM t_value_nullable;
DROP TABLE t_value_nullable;

DROP TABLE IF EXISTS t_value_array;
CREATE TABLE t_value_array (id UInt32, m Map(String, Array(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_value_array VALUES (1, {'a': [1, 2], 'b': []});
SELECT m FROM t_value_array;
DROP TABLE t_value_array;

DROP TABLE IF EXISTS t_value_lc;
CREATE TABLE t_value_lc (id UInt32, m Map(String, LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_value_lc VALUES (1, {'a': 'x', 'b': 'y'});
SELECT m FROM t_value_lc;
DROP TABLE t_value_lc;

-- Flipping the serialization version with existing data parts is rejected.
DROP TABLE IF EXISTS t_flip;
CREATE TABLE t_flip (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_flip VALUES (1, {'a': 1});
-- Flipping only one of the settings is rejected because they must agree.
ALTER TABLE t_flip MODIFY SETTING map_serialization_version = 'basic'; -- { serverError INVALID_SETTING_VALUE }
-- Flipping both at once with existing data parts is rejected.
ALTER TABLE t_flip MODIFY SETTING map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic'; -- { serverError SUPPORT_IS_DISABLED }
-- Flipping an empty table is fine.
DROP TABLE IF EXISTS t_flip_empty;
CREATE TABLE t_flip_empty (id UInt32, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns',
         map_serialization_version_for_zero_level_parts = 'with_key_columns';
ALTER TABLE t_flip_empty MODIFY SETTING map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic';
SELECT value FROM system.merge_tree_settings WHERE 0; -- keep client in sync
DROP TABLE t_flip_empty;
DROP TABLE t_flip;
