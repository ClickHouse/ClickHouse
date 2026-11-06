-- Subcolumn as the sign column of CollapsingMergeTree.

DROP TABLE IF EXISTS c_vertical;
DROP TABLE IF EXISTS c_horizontal;
DROP TABLE IF EXISTS c_json;
DROP TABLE IF EXISTS c_constraint;
DROP TABLE IF EXISTS c_materialized;

SELECT '-- Wrong subcolumns';
CREATE TABLE c_error (key UInt64, meta Tuple(sign Int8)) ENGINE = CollapsingMergeTree(meta.missing) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
CREATE TABLE c_error (key UInt64, meta Tuple(sign UInt8)) ENGINE = CollapsingMergeTree(meta.sign) ORDER BY key; -- { serverError BAD_TYPE_OF_FIELD }
-- VersionedCollapsingMergeTree supports only physical columns, because it adds the version column to the sorting key.
CREATE TABLE c_error (key UInt64, ver UInt64, meta Tuple(sign Int8)) ENGINE = VersionedCollapsingMergeTree(meta.sign, ver) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
CREATE TABLE c_error (key UInt64, sign Int8, meta Tuple(ver UInt64)) ENGINE = VersionedCollapsingMergeTree(sign, meta.ver) ORDER BY key; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT '-- Tuple element, vertical merge';
CREATE TABLE c_vertical (key UInt64, value String, meta Tuple(sign Int8, x UInt8))
ENGINE = CollapsingMergeTree(meta.sign) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- The rows of key 1 collapse on insert.
INSERT INTO c_vertical SETTINGS optimize_on_insert = 1 VALUES (1, 'a', (1, 0)), (1, 'a', (-1, 0)), (2, 'b', (1, 0));
SELECT * FROM c_vertical ORDER BY key;

INSERT INTO c_vertical SETTINGS optimize_on_insert = 0 VALUES (2, 'b', (-1, 0)), (2, 'c', (1, 0)), (3, 'd', (1, 0));
-- A part with a key range that does not intersect with other parts.
INSERT INTO c_vertical SETTINGS optimize_on_insert = 0 VALUES (10, 'x', (-1, 0)), (11, 'y', (1, 0));

SELECT * FROM c_vertical FINAL ORDER BY key;
SELECT key, value FROM c_vertical FINAL WHERE meta.sign = 1 ORDER BY key;

OPTIMIZE TABLE c_vertical FINAL;
SELECT * FROM c_vertical ORDER BY key;

SELECT '-- ALTER';
ALTER TABLE c_vertical MODIFY COLUMN meta Tuple(sign Int8, x UInt16);
SELECT * FROM c_vertical FINAL ORDER BY key;

ALTER TABLE c_vertical DROP COLUMN meta; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE c_vertical CLEAR COLUMN meta; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE c_vertical RENAME COLUMN meta TO meta2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE c_vertical MODIFY COLUMN meta Tuple(x UInt16); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE c_vertical MODIFY COLUMN meta Tuple(sign Int16, x UInt16); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE c_vertical UPDATE meta = (1, 1) WHERE 1; -- { serverError CANNOT_UPDATE_COLUMN }

DETACH TABLE c_vertical;
ATTACH TABLE c_vertical;
SELECT * FROM c_vertical FINAL ORDER BY key;

SELECT '-- Tuple element, horizontal merge';
CREATE TABLE c_horizontal (key UInt64, value String, meta Tuple(sign Int8, x UInt8))
ENGINE = CollapsingMergeTree(meta.sign) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 0;

INSERT INTO c_horizontal SETTINGS optimize_on_insert = 0 VALUES (1, 'a', (1, 0)), (2, 'b', (1, 0));
INSERT INTO c_horizontal SETTINGS optimize_on_insert = 0 VALUES (1, 'a', (-1, 0)), (2, 'b', (-1, 0)), (2, 'c', (1, 0));
OPTIMIZE TABLE c_horizontal FINAL;
SELECT * FROM c_horizontal ORDER BY key;

SELECT '-- JSON typed path';
CREATE TABLE c_json (key UInt64, j JSON(sign Int8))
ENGINE = CollapsingMergeTree(j.sign) ORDER BY key
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO c_json SETTINGS optimize_on_insert = 0 VALUES (1, '{"sign" : 1, "data" : "a"}'), (2, '{"sign" : 1, "data" : "b"}');
INSERT INTO c_json SETTINGS optimize_on_insert = 0 VALUES (1, '{"sign" : -1, "data" : "a"}');
SELECT * FROM c_json FINAL ORDER BY key;
OPTIMIZE TABLE c_json FINAL;
SELECT * FROM c_json ORDER BY key;

SELECT '-- Implicit sign constraint';
CREATE TABLE c_constraint (key UInt64, meta Tuple(sign Int8, x UInt8))
ENGINE = CollapsingMergeTree(meta.sign) ORDER BY key
SETTINGS add_implicit_sign_column_constraint_for_collapsing_engine = 1;

INSERT INTO c_constraint VALUES (1, (1, 0)), (2, (-1, 0));
INSERT INTO c_constraint VALUES (3, (2, 0)); -- { serverError VIOLATED_CONSTRAINT }
SELECT * FROM c_constraint ORDER BY key;

SELECT '-- MATERIALIZED column';
CREATE TABLE c_materialized (key UInt64, s Int8, meta Tuple(sign Int8) MATERIALIZED tuple(s))
ENGINE = CollapsingMergeTree(meta.sign) ORDER BY key
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO c_materialized (key, s) SETTINGS optimize_on_insert = 0 VALUES (1, 1), (1, -1);
-- Updating an input of the column would recompute the sign.
ALTER TABLE c_materialized UPDATE s = 1 WHERE s = -1; -- { serverError CANNOT_UPDATE_COLUMN }
UPDATE c_materialized SET s = 1 WHERE s = -1; -- { serverError CANNOT_UPDATE_COLUMN }
SELECT count() FROM c_materialized FINAL;

SELECT '-- Merge algorithms';
SYSTEM FLUSH LOGS part_log;
SELECT table, groupUniqArray(merge_algorithm) FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' AND table IN ('c_vertical', 'c_horizontal', 'c_json')
GROUP BY table ORDER BY table;

DROP TABLE c_vertical;
DROP TABLE c_horizontal;
DROP TABLE c_json;
DROP TABLE c_constraint;
DROP TABLE c_materialized;
