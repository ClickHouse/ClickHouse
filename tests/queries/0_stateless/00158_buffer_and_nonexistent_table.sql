
CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.mt_buffer_00158;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.mt_00158;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.mt_buffer_00158 (d Date DEFAULT today(), x UInt64) ENGINE = Buffer({CLICKHOUSE_DATABASE:Identifier}, mt_00158, 16, 100, 100, 1000000, 1000000, 1000000000, 1000000000);
SET send_logs_level = 'fatal'; -- Supress "Destination table test2.mt doesn't exist. Block of data is discarded."
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.mt_buffer_00158 (x) SELECT number AS x FROM system.numbers LIMIT 100000;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.mt_buffer_00158 (x) SELECT number AS x FROM system.numbers LIMIT 1000000;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.mt_buffer_00158;
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
