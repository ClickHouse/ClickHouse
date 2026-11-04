-- Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: the randomizer states this very setting, on top of what the test states

-- `allow_experimental_block_number_column` and `enable_block_number_column` are two names of one
-- setting, so a table holds one entry for it whichever name states it.

DROP TABLE IF EXISTS t_either_name;
CREATE TABLE t_either_name (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS allow_experimental_block_number_column = 1;

-- Only the entries for this setting, so that settings the server states of its own do not show up here
CREATE VIEW v_stored_setting AS
SELECT extractAll(engine_full, '(?:allow_experimental_|enable_)block_number_column = \\w+')
FROM system.tables WHERE database = currentDatabase() AND name = 't_either_name';

-- Writing it under its other name replaces the entry instead of adding a second one
ALTER TABLE t_either_name MODIFY SETTING enable_block_number_column = 0;
SELECT * FROM v_stored_setting;

-- And a reset under either name clears it
ALTER TABLE t_either_name MODIFY SETTING allow_experimental_block_number_column = 1;
SELECT * FROM v_stored_setting;
ALTER TABLE t_either_name RESET SETTING enable_block_number_column;
SELECT * FROM v_stored_setting;

-- A definition can state the setting under each of its names, and the last of them is in effect.
-- Writing it then leaves one entry behind, and resetting it leaves none.
DROP TABLE t_either_name;
CREATE TABLE t_either_name (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS allow_experimental_block_number_column = 1, enable_block_number_column = 0;
SELECT * FROM v_stored_setting;
ALTER TABLE t_either_name MODIFY SETTING enable_block_number_column = 1;
SELECT * FROM v_stored_setting;

DROP TABLE t_either_name;
CREATE TABLE t_either_name (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS allow_experimental_block_number_column = 1, enable_block_number_column = 0;
ALTER TABLE t_either_name RESET SETTING allow_experimental_block_number_column;
SELECT * FROM v_stored_setting;

DROP VIEW v_stored_setting;
DROP TABLE t_either_name;
