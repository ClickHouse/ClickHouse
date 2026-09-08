DROP TABLE IF EXISTS insert_expected_engine;
DROP TABLE IF EXISTS insert_expected_engine_log;
DROP TABLE IF EXISTS insert_expected_engine_wide;
DROP TABLE IF EXISTS insert_expected_engine_mv;
DROP TABLE IF EXISTS insert_expected_engine_mv_wide;
DROP TABLE IF EXISTS insert_expected_engine_dist;
DROP TABLE IF EXISTS insert_expected_engine_log_dist;
DROP TABLE IF EXISTS insert_expected_engine_mv_dist;

CREATE TABLE insert_expected_engine (x UInt8) ENGINE = Memory;
CREATE TABLE insert_expected_engine_log (x UInt8) ENGINE = Log;
-- A view target declaring `x` as another type: the requirement the named table consumed must not reach it.
CREATE TABLE insert_expected_engine_wide (x UInt16) ENGINE = Log;
CREATE MATERIALIZED VIEW insert_expected_engine_mv TO insert_expected_engine_log AS SELECT x FROM insert_expected_engine;
CREATE MATERIALIZED VIEW insert_expected_engine_mv_wide TO insert_expected_engine_wide AS SELECT toUInt16(x) AS x FROM insert_expected_engine;
CREATE TABLE insert_expected_engine_dist AS insert_expected_engine ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_engine);
-- A view onto a Distributed table: the requirement the named table consumed must not reach that shard.
CREATE TABLE insert_expected_engine_log_dist AS insert_expected_engine_log ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_engine_log);
CREATE MATERIALIZED VIEW insert_expected_engine_mv_dist TO insert_expected_engine_log_dist AS SELECT x FROM insert_expected_engine;

-- The table named is checked; the views' Log targets are its own writes and are not, even through a Distributed table.
INSERT INTO insert_expected_engine SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (1);
INSERT INTO insert_expected_engine SETTINGS insert_expected_table_engine = 'TimeSeries' VALUES (2); -- { serverError UNEXPECTED_TABLE_ENGINE }

-- The types the table declares are checked and consumed the same way: the view target declaring `x` as
-- UInt16 is never asked for the type the named table matched.
INSERT INTO insert_expected_engine SETTINGS insert_expected_column_types = {'x': 'UInt8'}, distributed_foreground_insert = 1 VALUES (3);
INSERT INTO insert_expected_engine SETTINGS insert_expected_column_types = {'x': 'UInt16'} VALUES (4); -- { serverError INCOMPATIBLE_SCHEMA }
-- A column the table does not declare at all.
INSERT INTO insert_expected_engine SETTINGS insert_expected_column_types = {'y': 'UInt8'} VALUES (5); -- { serverError INCOMPATIBLE_SCHEMA }
-- Both requirements on one INSERT.
INSERT INTO insert_expected_engine SETTINGS insert_expected_table_engine = 'Memory', insert_expected_column_types = {'x': 'UInt8'}, distributed_foreground_insert = 1 VALUES (6);

-- A Distributed table forwards both instead of checking itself: in-process to a local shard, and over the connection.
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (7);
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'TimeSeries', distributed_foreground_insert = 1 VALUES (8); -- { serverError UNEXPECTED_TABLE_ENGINE }
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'TimeSeries', distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (9); -- { serverError UNEXPECTED_TABLE_ENGINE }
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_column_types = {'x': 'UInt8'}, distributed_foreground_insert = 1 VALUES (10);
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_column_types = {'x': 'UInt16'}, distributed_foreground_insert = 1 VALUES (11); -- { serverError INCOMPATIBLE_SCHEMA }
-- Over the connection the shard reads the map back off the wire: a matching one still matches there.
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_column_types = {'x': 'UInt8'}, distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (12);
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_column_types = {'x': 'UInt16'}, distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (13); -- { serverError INCOMPATIBLE_SCHEMA }

SELECT * FROM insert_expected_engine ORDER BY x;
SELECT * FROM insert_expected_engine_log ORDER BY x;
SELECT * FROM insert_expected_engine_wide ORDER BY x;

DROP TABLE insert_expected_engine_mv_dist;
DROP TABLE insert_expected_engine_log_dist;
DROP TABLE insert_expected_engine_dist;
DROP TABLE insert_expected_engine_mv_wide;
DROP TABLE insert_expected_engine_mv;
DROP TABLE insert_expected_engine_wide;
DROP TABLE insert_expected_engine_log;
DROP TABLE insert_expected_engine;
