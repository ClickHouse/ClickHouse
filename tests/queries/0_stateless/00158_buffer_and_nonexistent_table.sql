-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS test2_00158;
DROP TABLE IF EXISTS test2_00158.mt_buffer_00158;
DROP TABLE IF EXISTS test2_00158.mt_00158;
CREATE TABLE test2_00158.mt_buffer_00158 (d Date DEFAULT today(), x UInt64) ENGINE = Buffer(test2_00158, mt_00158, 16, 100, 100, 1000000, 1000000, 1000000000, 1000000000);
SET send_logs_level = 'fatal'; -- Supress "Destination table test2.mt doesn't exist. Block of data is discarded."
INSERT INTO test2_00158.mt_buffer_00158 (x) SELECT number AS x FROM system.numbers LIMIT 100000;
INSERT INTO test2_00158.mt_buffer_00158 (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
DROP TABLE IF EXISTS test2_00158.mt_buffer_00158;
DROP DATABASE test2_00158;
