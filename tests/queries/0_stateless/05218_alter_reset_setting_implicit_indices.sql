-- Dropping a setting the implicit minmax indices depend on has to rebuild them, in every spelling.

DROP TABLE IF EXISTS t_reset_implicit_indices;

-- `ADD COLUMN` makes the `ALTER` write the metadata, which is what makes the stale indices visible.
CREATE TABLE t_reset_implicit_indices (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, add_minmax_index_for_block_number_column = 1, add_minmax_index_for_numeric_columns = 0;

SELECT 'index after CREATE', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_reset_implicit_indices';

ALTER TABLE t_reset_implicit_indices ADD COLUMN b UInt64, MODIFY SETTING enable_block_number_column = DEFAULT;

SELECT 'index after MODIFY SETTING = DEFAULT', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_reset_implicit_indices';

DROP TABLE t_reset_implicit_indices;


-- The same through `RESET SETTING`.
CREATE TABLE t_reset_implicit_indices (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, add_minmax_index_for_block_number_column = 1, add_minmax_index_for_numeric_columns = 0;

ALTER TABLE t_reset_implicit_indices ADD COLUMN b UInt64, RESET SETTING enable_block_number_column;

SELECT 'index after RESET SETTING', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_reset_implicit_indices';

DROP TABLE t_reset_implicit_indices;


-- A change plus a drop is split in two commands, so the rebuild must run after the drop.
CREATE TABLE t_reset_implicit_indices (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1, add_minmax_index_for_block_number_column = 1, add_minmax_index_for_numeric_columns = 0;

ALTER TABLE t_reset_implicit_indices
    ADD COLUMN b UInt64,
    MODIFY SETTING min_bytes_for_wide_part = 7, enable_block_number_column = DEFAULT;

SELECT 'index after mixed change and reset', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_reset_implicit_indices';

DROP TABLE t_reset_implicit_indices;
