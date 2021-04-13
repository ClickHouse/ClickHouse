-- There is different code path for the following cases:
SELECT * FROM system.parts FORMAT Null;
SELECT *, _state FROM system.parts FORMAT Null;
SELECT _state FROM system.parts FORMAT Null;
SELECT *, _use_count FROM system.parts FORMAT Null;
SELECT *, _state, _use_count FROM system.parts FORMAT Null;
SELECT _state, _use_count FROM system.parts FORMAT Null;
SELECT _use_count FROM system.parts FORMAT Null;
-- And same for system.parts_columns
SELECT * FROM system.parts_columns FORMAT Null;
SELECT *, _state FROM system.parts_columns FORMAT Null;
SELECT _state FROM system.parts_columns FORMAT Null;
SELECT *, _use_count FROM system.parts_columns FORMAT Null;
SELECT *, _state, _use_count FROM system.parts_columns FORMAT Null;
SELECT _state, _use_count FROM system.parts_columns FORMAT Null;
SELECT _use_count FROM system.parts_columns FORMAT Null;

-- Create one table and see some columns in system.parts
DROP TABLE IF EXISTS data_01660;
CREATE TABLE data_01660 (key Int) Engine=MergeTree() ORDER BY key;
SYSTEM STOP MERGES data_01660;
SELECT _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';

-- Empty
SELECT _state, _use_count FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';

-- Add part and check again
SELECT '# two parts';
INSERT INTO data_01660 VALUES (0);
INSERT INTO data_01660 VALUES (1);
-- NOTE that use_count is at least 2 (due to system.parts also holds one "reference")
SELECT _state, _use_count FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';

-- OPTIMIZE to create Outdated parts
SELECT '# optimize';
SYSTEM START MERGES data_01660;
OPTIMIZE TABLE data_01660 FINAL;
SELECT count(), _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660' GROUP BY _state;

-- TRUNCATE does not remove parts instantly
SELECT '# truncate';
TRUNCATE data_01660;
SELECT _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';

-- But DROP does
SELECT '# drop';
DROP TABLE data_01660;
SELECT * FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
