CREATE TABLE data_02716_1 (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE data_02716_2 (v UInt64) ENGINE = MergeTree ORDER BY v;

CREATE DATABASE db_02716;
CREATE TABLE db_02716.data_02716_3 (v UInt64) ENGINE = MergeTree ORDER BY v;

INSERT INTO data_02716_1 SELECT * FROM system.numbers LIMIT 1;

-- { echoOn }
DROP TABLE IF EMPTY data_02716_2;
DROP TABLE IF EMPTY data_02716_1; -- { serverError TABLE_NOT_EMPTY }
TRUNCATE TABLE data_02716_1;
DROP TABLE IF EMPTY data_02716_1;
DROP DATABASE IF EMPTY db_02716; -- { serverError NOT_IMPLEMENTED }

SELECT count() FROM system.tables WHERE database = 'db_02716';
SELECT count() FROM system.tables WHERE database = 'default' AND name IN ('data_02716_1', 'data_02716_2');
