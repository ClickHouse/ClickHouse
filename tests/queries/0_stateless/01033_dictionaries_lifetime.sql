SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict Engine = Ordinary;

DROP TABLE IF EXISTS database_for_dict.table_for_dict;

CREATE TABLE database_for_dict.table_for_dict
(
  key_column UInt64,
  second_column UInt8,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO database_for_dict.table_for_dict VALUES (1, 100, 'Hello world');

DROP DATABASE IF EXISTS ordinary_db;

CREATE DATABASE ordinary_db ENGINE = Ordinary;

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

CREATE DICTIONARY ordinary_db.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetUInt8('ordinary_db.dict1', 'second_column', toUInt64(100500));

SELECT lifetime_min, lifetime_max FROM system.dictionaries WHERE name = 'dict1';

DROP DICTIONARY IF EXISTS ordinary_db.dict1;

DROP DATABASE IF EXISTS ordinary_db;

DROP TABLE IF EXISTS database_for_dict.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict;

