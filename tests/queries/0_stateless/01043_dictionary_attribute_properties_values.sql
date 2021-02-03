DROP DATABASE IF EXISTS dictdb_01043;
CREATE DATABASE dictdb_01043;

CREATE TABLE dictdb_01043.dicttbl(key Int64, value_default String, value_expression String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dictdb_01043.dicttbl VALUES (12, 'hello', '55:66:77');


CREATE DICTIONARY dictdb_01043.dict
(
  key Int64 DEFAULT -1,
  value_default String DEFAULT 'world',
  value_expression String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'

)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dicttbl' DB 'dictdb_01043'))
LAYOUT(FLAT())
LIFETIME(1);


SELECT dictGetString('dictdb_01043.dict', 'value_default', toUInt64(12));
SELECT dictGetString('dictdb_01043.dict', 'value_default', toUInt64(14));

SELECT dictGetString('dictdb_01043.dict', 'value_expression', toUInt64(12));
SELECT dictGetString('dictdb_01043.dict', 'value_expression', toUInt64(14));

DROP DATABASE IF EXISTS dictdb_01043;
