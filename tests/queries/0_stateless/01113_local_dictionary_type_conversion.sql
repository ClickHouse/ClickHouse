-- Tags: no-parallel

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

CREATE TABLE database_for_dict.table_for_dict (
  CompanyID String,
  OSType Enum('UNKNOWN' = 0, 'WINDOWS' = 1, 'LINUX' = 2, 'ANDROID' = 3, 'MAC' = 4),
  SomeID Int32
)
ENGINE = Memory();

INSERT INTO database_for_dict.table_for_dict VALUES ('First', 'WINDOWS', 1), ('Second', 'LINUX', 2);

CREATE DICTIONARY database_for_dict.dict_with_conversion
(
  CompanyID String DEFAULT '',
  OSType String DEFAULT '',
  SomeID Int32 DEFAULT 0
)
PRIMARY KEY CompanyID
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 20)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT * FROM database_for_dict.dict_with_conversion ORDER BY CompanyID;

DROP DATABASE IF EXISTS database_for_dict;
