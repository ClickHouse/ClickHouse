-- Tags: no-random-merge-tree-settings
-- `enable_block_number_column` / `enable_block_offset_column` are part of what is verified here,
-- so the test must not run with randomized MergeTree settings.

CREATE TABLE sorting_key_reader_virtual (x UInt8)
ENGINE = MergeTree
ORDER BY _part; -- { serverError BAD_ARGUMENTS }

CREATE TABLE sorting_key_block_virtual_disabled (x UInt8)
ENGINE = MergeTree
ORDER BY (_block_number, _block_offset); -- { serverError BAD_ARGUMENTS }

-- `_block_number` is materialized with a placeholder at insert time and the primary index of a
-- level-0 part is repaired at load; the repair works only for the bare key column, so key
-- expressions over `_block_number` are rejected.
CREATE TABLE sorting_key_block_virtual_expression (x UInt8)
ENGINE = MergeTree
ORDER BY (_block_number + 1)
SETTINGS enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE sorting_key_block_virtual_mixed_expression (x UInt8)
ENGINE = MergeTree
ORDER BY (x, sipHash64(_block_number))
SETTINGS enable_block_number_column = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE sorting_key_block_virtual_enabled (x UInt8)
ENGINE = MergeTree
ORDER BY (_block_number, _block_offset)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO sorting_key_block_virtual_enabled VALUES (1);
INSERT INTO sorting_key_block_virtual_enabled VALUES (2);
OPTIMIZE TABLE sorting_key_block_virtual_enabled FINAL;
SELECT x FROM sorting_key_block_virtual_enabled ORDER BY x;

-- The sorting key requires the columns to stay materialized, so the settings cannot be turned
-- off after creation.
ALTER TABLE sorting_key_block_virtual_enabled MODIFY SETTING enable_block_number_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE sorting_key_block_virtual_enabled MODIFY SETTING enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE sorting_key_block_virtual_enabled RESET SETTING enable_block_number_column; -- { serverError BAD_ARGUMENTS }
ALTER TABLE sorting_key_block_virtual_enabled RESET SETTING enable_block_offset_column; -- { serverError BAD_ARGUMENTS }
-- Re-enabling is a no-op and must be allowed.
ALTER TABLE sorting_key_block_virtual_enabled MODIFY SETTING enable_block_number_column = 1;

DROP TABLE sorting_key_block_virtual_enabled;

-- A table whose sorting key does not use the columns can disable them freely.
CREATE TABLE sorting_key_no_virtual (x UInt8)
ENGINE = MergeTree
ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

ALTER TABLE sorting_key_no_virtual MODIFY SETTING enable_block_number_column = 0;
ALTER TABLE sorting_key_no_virtual RESET SETTING enable_block_offset_column;

DROP TABLE sorting_key_no_virtual;
