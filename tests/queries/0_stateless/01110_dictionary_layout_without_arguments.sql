-- Tags: no-parallel

DROP DATABASE IF EXISTS db_for_dict;
CREATE DATABASE db_for_dict;

CREATE TABLE db_for_dict.table_for_dict
(
  key1 UInt64,
  value String
)
ENGINE = Memory();

INSERT INTO db_for_dict.table_for_dict VALUES (1, 'Hello'), (2, 'World');

CREATE DICTIONARY db_for_dict.dict_with_hashed_layout
(
  key1 UInt64,
  value String
)
PRIMARY KEY key1
LAYOUT(HASHED)
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'db_for_dict'))
LIFETIME(MIN 1 MAX 10);

SELECT dictGet('db_for_dict.dict_with_hashed_layout', 'value', toUInt64(2));

DETACH DICTIONARY db_for_dict.dict_with_hashed_layout;

ATTACH DICTIONARY db_for_dict.dict_with_hashed_layout;

SHOW CREATE DICTIONARY db_for_dict.dict_with_hashed_layout;

SELECT dictGet('db_for_dict.dict_with_hashed_layout', 'value', toUInt64(1));

DROP DATABASE IF EXISTS db_for_dict;
