DROP TABLE IF EXISTS insert_expected_engine;
DROP TABLE IF EXISTS insert_expected_engine_log;
DROP TABLE IF EXISTS insert_expected_engine_mv;
DROP TABLE IF EXISTS insert_expected_engine_dist;
DROP TABLE IF EXISTS insert_expected_engine_log_dist;
DROP TABLE IF EXISTS insert_expected_engine_mv_dist;

CREATE TABLE insert_expected_engine (x UInt8) ENGINE = Memory;
CREATE TABLE insert_expected_engine_log (x UInt8) ENGINE = Log;
CREATE MATERIALIZED VIEW insert_expected_engine_mv TO insert_expected_engine_log AS SELECT x FROM insert_expected_engine;
CREATE TABLE insert_expected_engine_dist AS insert_expected_engine ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_engine);
-- A view onto a Distributed table: the requirement the named table consumed must not reach that shard.
CREATE TABLE insert_expected_engine_log_dist AS insert_expected_engine_log ENGINE = Distributed(test_shard_localhost, currentDatabase(), insert_expected_engine_log);
CREATE MATERIALIZED VIEW insert_expected_engine_mv_dist TO insert_expected_engine_log_dist AS SELECT x FROM insert_expected_engine;

-- The table named is checked; the views' Log targets are its own writes and are not, even through a Distributed table.
INSERT INTO insert_expected_engine SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (1);
INSERT INTO insert_expected_engine SETTINGS insert_expected_table_engine = 'TimeSeries' VALUES (2); -- { serverError UNEXPECTED_TABLE_ENGINE }

-- A Distributed table forwards the setting: in-process to a local shard, and over the connection.
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'Memory', distributed_foreground_insert = 1 VALUES (3);
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'TimeSeries', distributed_foreground_insert = 1 VALUES (4); -- { serverError UNEXPECTED_TABLE_ENGINE }
INSERT INTO insert_expected_engine_dist SETTINGS insert_expected_table_engine = 'TimeSeries', distributed_foreground_insert = 1, prefer_localhost_replica = 0 VALUES (5); -- { serverError UNEXPECTED_TABLE_ENGINE }

SELECT * FROM insert_expected_engine ORDER BY x;
SELECT * FROM insert_expected_engine_log ORDER BY x;

DROP TABLE insert_expected_engine_mv_dist;
DROP TABLE insert_expected_engine_log_dist;
DROP TABLE insert_expected_engine_dist;
DROP TABLE insert_expected_engine_mv;
DROP TABLE insert_expected_engine_log;
DROP TABLE insert_expected_engine;
