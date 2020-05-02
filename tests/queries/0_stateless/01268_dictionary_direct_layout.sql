DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict Engine = Ordinary;

DROP TABLE IF EXISTS database_for_dict.table_for_dict1;

DROP TABLE IF EXISTS database_for_dict.table_for_dict2;

CREATE TABLE database_for_dict.table_for_dict1
(
  key_column UInt64,
  second_column UInt64,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict.table_for_dict1 VALUES (100500, 10000000, 'Hello world');

CREATE TABLE database_for_dict.table_for_dict2
(
  region_id UInt64,
  parent_region UInt64,
  region_name String
)
ENGINE = MergeTree()
ORDER BY region_id;

INSERT INTO database_for_dict.table_for_dict2 VALUES (1, 0, 'Russia');
INSERT INTO database_for_dict.table_for_dict2 VALUES (2, 1, 'Moscow');
INSERT INTO database_for_dict.table_for_dict2 VALUES (3, 2, 'Center');
INSERT INTO database_for_dict.table_for_dict2 VALUES (4, 0, 'Great Britain');
INSERT INTO database_for_dict.table_for_dict2 VALUES (5, 4, 'London');

DROP DATABASE IF EXISTS ordinary_db;

CREATE DATABASE ordinary_db ENGINE = Ordinary;

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

CREATE DICTIONARY ordinary_db.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt64 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict1' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 600 MAX 600)
LAYOUT(DIRECT()) SETTINGS(max_result_bytes=1);

CREATE DICTIONARY ordinary_db.dict2
(
  region_id UInt64 DEFAULT 0,
  parent_region UInt64 DEFAULT 0 HIERARCHICAL,
  region_name String DEFAULT ''
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict2' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 600 MAX 600)
LAYOUT(DIRECT());

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetHierarchy('ordinary_db.dict2', toUInt64(3));
SELECT dictHas('ordinary_db.dict2', toUInt64(3));
SELECT dictHas('ordinary_db.dict2', toUInt64(45));
SELECT dictIsIn('ordinary_db.dict2', toUInt64(3), toUInt64(1));
SELECT dictIsIn('ordinary_db.dict2', toUInt64(1), toUInt64(3));
SELECT dictGetUInt64('ordinary_db.dict1', 'second_column', toUInt64(100500)); -- { serverError 396 }

SELECT 'END';

DROP DICTIONARY IF EXISTS ordinary_db.dict1;
DROP DICTIONARY IF EXISTS ordinary_db.dict2;

DROP DATABASE IF EXISTS ordinary_db;

DROP TABLE IF EXISTS database_for_dict.table_for_dict1;
DROP TABLE IF EXISTS database_for_dict.table_for_dict2;

DROP DATABASE IF EXISTS database_for_dict;
