DROP DATABASE IF EXISTS db_01391;
CREATE DATABASE db_01391;
USE db_01391;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d_src;
DROP DICTIONARY IF EXISTS d;

CREATE TABLE t (click_city_id UInt32, click_country_id UInt32) Engine = Memory;
CREATE TABLE d_src (id UInt64, country_id UInt8, name String) Engine = Memory;

INSERT INTO t VALUES (0, 0);
INSERT INTO d_src VALUES (0, 0, 'n');

CREATE DICTIONARY d (id UInt32, country_id UInt8, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB 'db_01391' table 'd_src'))
LIFETIME(MIN 1 MAX 1)
LAYOUT(HASHED());

select click_country_id from t cc
left join d on toUInt32(d.id) = cc.click_city_id;

DROP DICTIONARY d;
DROP TABLE t;
DROP TABLE d_src;
DROP DATABASE IF EXISTS db_01391;
