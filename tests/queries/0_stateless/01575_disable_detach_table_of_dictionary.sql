-- Tags: no-parallel

DROP DATABASE IF EXISTS database_for_dict;

CREATE DATABASE database_for_dict;

CREATE TABLE database_for_dict.table_for_dict (k UInt64, v UInt8) ENGINE = MergeTree ORDER BY k;

DROP DICTIONARY IF EXISTS database_for_dict.dict1;

CREATE DICTIONARY database_for_dict.dict1 (k UInt64 DEFAULT 0, v UInt8 DEFAULT 1) PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB 'database_for_dict'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

DETACH TABLE database_for_dict.dict1; -- { serverError CANNOT_DETACH_DICTIONARY_AS_TABLE }

DETACH DICTIONARY database_for_dict.dict1;

ATTACH TABLE database_for_dict.dict1; -- { serverError INCORRECT_QUERY }

ATTACH DICTIONARY database_for_dict.dict1;

DROP DICTIONARY database_for_dict.dict1;

DROP TABLE database_for_dict.table_for_dict;

DROP DATABASE IF EXISTS database_for_dict;
