CREATE DATABASE IF NOT EXISTS test2;
DROP TABLE IF EXISTS test2.mt_buffer;
CREATE TABLE test2.mt_buffer (d Date DEFAULT today(), x UInt64) ENGINE = Buffer(test2, mt, 16, 100, 100, 1000000, 1000000, 1000000000, 1000000000);
SET send_logs_level = 'none'; -- Supress "Destination table test2.mt doesn't exist. Block of data is discarded."
INSERT INTO test2.mt_buffer (x) SELECT number AS x FROM system.numbers LIMIT 100000;
INSERT INTO test2.mt_buffer (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
DROP DATABASE test2;
