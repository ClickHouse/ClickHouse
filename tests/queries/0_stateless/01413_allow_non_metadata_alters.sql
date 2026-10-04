DROP TABLE IF EXISTS non_metadata_alters;

CREATE TABLE non_metadata_alters (
  key UInt64,
  value1 String,
  value2 Enum8('Hello' = 1, 'World' = 2),
  value3 UInt16,
  value4 DateTime,
  value5 Date
)
ENGINE = MergeTree()
ORDER BY tuple();


SET allow_non_metadata_alters = 0;

ALTER TABLE non_metadata_alters MODIFY COLUMN value3 UInt64; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MODIFY COLUMN value1 UInt32; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MODIFY COLUMN value4 Date; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters DROP COLUMN value4; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MODIFY COLUMN value2 Enum8('x' = 5, 'y' = 6); --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters RENAME COLUMN value4 TO renamed_value4; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MODIFY COLUMN value3 UInt16 TTL value5 + INTERVAL 5 DAY; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

SET materialize_ttl_after_modify = 0;

ALTER TABLE non_metadata_alters MODIFY COLUMN value3 UInt16 TTL value5 + INTERVAL 5 DAY;

SHOW CREATE TABLE non_metadata_alters;

ALTER TABLE non_metadata_alters MODIFY COLUMN value1 String DEFAULT 'X';

ALTER TABLE non_metadata_alters MODIFY COLUMN value2 Enum8('Hello' = 1, 'World' = 2, '!' = 3);

ALTER TABLE non_metadata_alters MODIFY COLUMN value3 Date;

ALTER TABLE non_metadata_alters MODIFY COLUMN value4 UInt32;

ALTER TABLE non_metadata_alters ADD COLUMN value6 Decimal(3, 3);

SHOW CREATE TABLE non_metadata_alters;

-- Commands that reach the storage as explicit mutations rather than as alter commands.
-- They rewrite parts too, so the same setting must refuse them.

-- Both modes decide whether a statement below is a heavyweight mutation at all, so pin them.
SET alter_update_mode = 'heavy';
SET lightweight_delete_mode = 'alter_update';

-- Lightweight updates need materialized _block_number and _block_offset columns.
ALTER TABLE non_metadata_alters MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE non_metadata_alters ADD INDEX idx_value3 value3 TYPE minmax GRANULARITY 1;

ALTER TABLE non_metadata_alters ADD STATISTICS key TYPE tdigest;

ALTER TABLE non_metadata_alters ADD PROJECTION proj_key (SELECT key ORDER BY key);

ALTER TABLE non_metadata_alters ADD COLUMN value7 UInt64 DEFAULT key + 100;

ALTER TABLE non_metadata_alters UPDATE value7 = 1 WHERE key = 1; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters DELETE WHERE key = 999; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MATERIALIZE INDEX idx_value3; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MATERIALIZE STATISTICS key; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MATERIALIZE PROJECTION proj_key; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MATERIALIZE COLUMN value7; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters MATERIALIZE TTL; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters APPLY DELETED MASK; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters APPLY PATCHES; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

ALTER TABLE non_metadata_alters REWRITE PARTS; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

-- A user-written assignment cannot borrow the lightweight-delete exemption, which is
-- granted by the interpreter that synthesizes the rewrite rather than by the statement shape.
ALTER TABLE non_metadata_alters UPDATE `_row_exists` = 0 WHERE key = 1; --{serverError ALTER_OF_COLUMN_IS_FORBIDDEN}

-- The setting governs ALTER DDL, so the dedicated delete and update statements stay permitted
-- in every mode. The projection has to go first: lightweight_mutation_projection_mode refuses
-- them on a table that has one.
ALTER TABLE non_metadata_alters DROP PROJECTION proj_key SETTINGS allow_non_metadata_alters = 1;

DELETE FROM non_metadata_alters WHERE key = 999;

DELETE FROM non_metadata_alters WHERE key = 998 SETTINGS lightweight_delete_mode = 'lightweight_update';

UPDATE non_metadata_alters SET value7 = 2 WHERE key = 1;

-- An ALTER UPDATE that runs as a lightweight update never reaches the mutation path, so it is
-- permitted even though the same statement is refused under the default 'heavy' mode above.
ALTER TABLE non_metadata_alters UPDATE value7 = 4 WHERE key = 1 SETTINGS alter_update_mode = 'lightweight';

ALTER TABLE non_metadata_alters UPDATE value7 = 5 WHERE key = 1 SETTINGS alter_update_mode = 'lightweight_force';

-- The commands refused above run when the setting is enabled, which is the default. Two are not
-- repeated here: the projection was dropped above so that `DELETE FROM` is permitted at all, and
-- the `_row_exists` assignment is only interesting in the refused direction.

SET allow_non_metadata_alters = 1;

ALTER TABLE non_metadata_alters UPDATE value7 = 3 WHERE key = 1;

ALTER TABLE non_metadata_alters DELETE WHERE key = 997;

ALTER TABLE non_metadata_alters MATERIALIZE INDEX idx_value3;

ALTER TABLE non_metadata_alters MATERIALIZE STATISTICS key;

ALTER TABLE non_metadata_alters MATERIALIZE COLUMN value7;

ALTER TABLE non_metadata_alters MATERIALIZE TTL;

ALTER TABLE non_metadata_alters APPLY DELETED MASK;

ALTER TABLE non_metadata_alters APPLY PATCHES;

ALTER TABLE non_metadata_alters REWRITE PARTS;

SELECT count() FROM non_metadata_alters;

DROP TABLE IF EXISTS non_metadata_alters;
