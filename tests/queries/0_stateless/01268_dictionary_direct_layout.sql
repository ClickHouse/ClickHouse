-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS database_for_dict_01268;

CREATE DATABASE database_for_dict_01268;

DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict1;
DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict2;
DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict3;

CREATE TABLE database_for_dict_01268.table_for_dict1
(
  key_column UInt64,
  second_column UInt64,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict_01268.table_for_dict1 VALUES (100500, 10000000, 'Hello world');

CREATE TABLE database_for_dict_01268.table_for_dict2
(
  region_id UInt64,
  parent_region UInt64,
  region_name String
)
ENGINE = MergeTree()
ORDER BY region_id;

INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (1, 0, 'Russia');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (2, 1, 'Moscow');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (3, 2, 'Center');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (4, 0, 'Great Britain');
INSERT INTO database_for_dict_01268.table_for_dict2 VALUES (5, 4, 'London');

CREATE TABLE database_for_dict_01268.table_for_dict3
(
  region_id UInt64,
  parent_region Float32,
  region_name String
)
ENGINE = MergeTree()
ORDER BY region_id;

INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (1, 0.5, 'Russia');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (2, 1.6, 'Moscow');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (3, 2.3, 'Center');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (4, 0.2, 'Great Britain');
INSERT INTO database_for_dict_01268.table_for_dict3 VALUES (5, 4.9, 'London');

DROP DATABASE IF EXISTS db_01268;

CREATE DATABASE db_01268;

DROP DICTIONARY IF EXISTS db_01268.dict1;
DROP DICTIONARY IF EXISTS db_01268.dict2;
DROP DICTIONARY IF EXISTS db_01268.dict3;

CREATE DICTIONARY db_01268.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt64 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT()) SETTINGS(max_result_bytes=1);

CREATE DICTIONARY db_01268.dict2
(
  region_id UInt64 DEFAULT 0,
  parent_region UInt64 DEFAULT 0 HIERARCHICAL,
  region_name String DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT()) SETTINGS(dictionary_use_async_executor=1, max_threads=8);

CREATE DICTIONARY db_01268.dict3
(
  region_id UInt64 DEFAULT 0,
  parent_region Float32 DEFAULT 0,
  region_name String DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict3' PASSWORD '' DB 'database_for_dict_01268'))
LAYOUT(DIRECT());

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetHierarchy('db_01268.dict2', toUInt64(3));
SELECT dictHas('db_01268.dict2', toUInt64(3));
SELECT dictHas('db_01268.dict2', toUInt64(45));
SELECT dictIsIn('db_01268.dict2', toUInt64(3), toUInt64(1));
SELECT dictIsIn('db_01268.dict2', toUInt64(1), toUInt64(3));
SELECT dictGetUInt64('db_01268.dict2', 'parent_region', toUInt64(3));
SELECT dictGetUInt64('db_01268.dict2', 'parent_region', toUInt64(99));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', toUInt64(3));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', toUInt64(2));
SELECT dictGetFloat32('db_01268.dict3', 'parent_region', toUInt64(1));
SELECT dictGetString('db_01268.dict2', 'region_name', toUInt64(5));
SELECT dictGetString('db_01268.dict2', 'region_name', toUInt64(4));
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(100), 'NONE');

SELECT number + 1, dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(number + 1), 'NONE') chars FROM numbers(10);
SELECT number + 1, dictGetFloat32OrDefault('db_01268.dict3', 'parent_region', toUInt64(number + 1), toFloat32(0)) chars FROM numbers(10);
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(1), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(2), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(3), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(4), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(5), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(6), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(7), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(8), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(9), 'NONE');
SELECT dictGetStringOrDefault('db_01268.dict2', 'region_name', toUInt64(10), 'NONE');

SELECT dictGetUInt64('db_01268.dict1', 'second_column', toUInt64(100500)); -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT 'END';

DROP DICTIONARY IF EXISTS db_01268.dict1;
DROP DICTIONARY IF EXISTS db_01268.dict2;
DROP DICTIONARY IF EXISTS db_01268.dict3;

DROP DATABASE IF EXISTS db_01268;

DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict1;
DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict2;
DROP TABLE IF EXISTS database_for_dict_01268.table_for_dict3;

DROP DATABASE IF EXISTS database_for_dict_01268;
