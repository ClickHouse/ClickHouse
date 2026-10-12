-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t SYNC;

-- Both block-number/offset columns disabled: must reject `with_block_number_offset`
CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS part_minmax_index_columns = 'with_block_number_offset'; -- { serverError BAD_ARGUMENTS }

-- `enable_block_offset_column = 1` alone is not enough
CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset'; -- { serverError BAD_ARGUMENTS }

-- `enable_block_number_column = 1, enable_block_offset_column = 1` should be both to enable min-max
CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_minmax_index_columns = 'with_block_number_offset';

-- ALTER that disables both columns while the setting is `with_block_number_offset` must fail
ALTER TABLE t MODIFY SETTING enable_block_number_column = 0, enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }

-- ALTER that drops the setting back to `partition_key_only` is fine even with both columns off
ALTER TABLE t MODIFY SETTING part_minmax_index_columns = 'partition_key_only';
ALTER TABLE t MODIFY SETTING enable_block_number_column = 0, enable_block_offset_column = 0;

DROP TABLE IF EXISTS t SYNC;

-- Default value (`partition_key_only`) does not require the persisted columns
CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;
DROP TABLE IF EXISTS t SYNC;

SELECT 'ok';
