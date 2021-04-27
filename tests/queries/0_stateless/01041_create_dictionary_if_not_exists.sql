DROP TABLE IF EXISTS dictdb.table_for_dict;
DROP DICTIONARY IF EXISTS dictdb.dict_exists;
DROP DATABASE IF EXISTS dictdb;

CREATE DATABASE dictdb;

CREATE TABLE dictdb.table_for_dict
(
  key_column UInt64,
  value Float64
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO dictdb.table_for_dict VALUES (1, 1.1);

CREATE DICTIONARY IF NOT EXISTS dictdb.dict_exists
(
  key_column UInt64,
  value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'dictdb'))
LIFETIME(1)
LAYOUT(FLAT());

SELECT dictGetFloat64('dictdb.dict_exists', 'value', toUInt64(1));


CREATE DICTIONARY IF NOT EXISTS dictdb.dict_exists
(
  key_column UInt64,
  value Float64 DEFAULT 77.77
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB 'dictdb'))
LIFETIME(1)
LAYOUT(FLAT());

SELECT dictGetFloat64('dictdb.dict_exists', 'value', toUInt64(1));

DROP TABLE dictdb.table_for_dict;
DROP DICTIONARY dictdb.dict_exists;
DROP DATABASE dictdb;
