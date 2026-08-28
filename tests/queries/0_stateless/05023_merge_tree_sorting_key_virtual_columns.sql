-- Tags: no-random-merge-tree-settings
-- `enable_block_number_column` / `enable_block_offset_column` are part of what is verified here,
-- so the test must not run with randomized MergeTree settings.

CREATE TABLE sorting_key_reader_virtual (x UInt8)
ENGINE = MergeTree
ORDER BY _part; -- { serverError BAD_ARGUMENTS }

CREATE TABLE sorting_key_block_virtual_disabled (x UInt8)
ENGINE = MergeTree
ORDER BY (_block_number, _block_offset); -- { serverError BAD_ARGUMENTS }

CREATE TABLE sorting_key_block_virtual_enabled (x UInt8)
ENGINE = MergeTree
ORDER BY (_block_number, _block_offset)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO sorting_key_block_virtual_enabled VALUES (1);
INSERT INTO sorting_key_block_virtual_enabled VALUES (2);
OPTIMIZE TABLE sorting_key_block_virtual_enabled FINAL;
SELECT x FROM sorting_key_block_virtual_enabled ORDER BY x;

DROP TABLE sorting_key_block_virtual_enabled;
