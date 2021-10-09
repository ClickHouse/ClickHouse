-- Tags: no-parallel

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS lv;

CREATE TABLE t1 (key Int) Engine=Memory;
CREATE TABLE t2 AS t1;
DROP TABLE t2;
CREATE TABLE t2 Engine=Memory AS t1;
DROP TABLE t2;
CREATE TABLE t2 AS t1 Engine=Memory;
DROP TABLE t2;
CREATE TABLE t3 AS numbers(10);
DROP TABLE t3;

-- live view
SET allow_experimental_live_view=1;
CREATE LIVE VIEW lv AS SELECT * FROM t1;
CREATE TABLE t3 AS lv; -- { serverError 80; }
DROP TABLE lv;

-- view
CREATE VIEW v AS SELECT * FROM t1;
CREATE TABLE t3 AS v; -- { serverError 80; }
DROP TABLE v;

-- dictionary
DROP DICTIONARY IF EXISTS dict;
DROP DATABASE if exists test_01056_dict_data;
CREATE DATABASE test_01056_dict_data;
CREATE TABLE test_01056_dict_data.dict_data (key Int, value UInt16) Engine=Memory();
CREATE DICTIONARY dict
(
    `key` UInt64,
    `value` UInt16
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1' PORT tcpPort()
    TABLE 'dict_data' DB 'test_01056_dict_data' USER 'default' PASSWORD ''))
LIFETIME(MIN 0 MAX 0)
LAYOUT(SPARSE_HASHED());
CREATE TABLE t3 AS dict; -- { serverError 80; }

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t3;
DROP DICTIONARY dict;
DROP TABLE test_01056_dict_data.dict_data;

DROP DATABASE test_01056_dict_data;

CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
DROP TABLE t1;
