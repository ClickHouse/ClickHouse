-- There is different code path when:
-- - _state is not requested
-- - _state is requested
-- - only _state is requested
SELECT * FROM system.parts FORMAT Null;
SELECT *, _state FROM system.parts FORMAT Null;
SELECT _state FROM system.parts FORMAT Null;

-- Create one table and see some columns in system.parts
DROP TABLE IF EXISTS data_01660;
CREATE TABLE data_01660 (key Int) Engine=MergeTree() ORDER BY key;
SYSTEM STOP MERGES data_01660;

-- Empty
SELECT _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';

-- Add part and check again
SELECT '# two parts';
INSERT INTO data_01660 VALUES (0);
INSERT INTO data_01660 VALUES (1);
SELECT _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
SELECT name, _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660' ORDER BY name;
SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660' ORDER BY name;

-- OPTIMIZE to create Outdated parts
SELECT '# optimize';
SYSTEM START MERGES data_01660;
OPTIMIZE TABLE data_01660 FINAL;
SELECT count(), _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660' GROUP BY _state ORDER BY _state;

-- TRUNCATE does not remove parts instantly
-- Empty active parts are clearing by async process
-- Inactive parts are clearing by async process also
SELECT '# truncate';
TRUNCATE data_01660;
SELECT if (count() > 0, 'HAVE PARTS', 'NO PARTS'), _state FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660' GROUP BY _state ORDER BY _state;

-- But DROP does
SELECT '# drop';
DROP TABLE data_01660;
SELECT * FROM system.parts WHERE database = currentDatabase() AND table = 'data_01660';
