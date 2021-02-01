DROP DATABASE IF EXISTS dictdb;
CREATE DATABASE dictdb;

CREATE TABLE dictdb.dicttbl(key Int64, value_default String, value_expression String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dictdb.dicttbl VALUES (12, 'hello', '55:66:77');


CREATE DICTIONARY dictdb.dict
(
  key Int64 DEFAULT -1,
  value_default String DEFAULT 'world',
  value_expression String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'

)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LAYOUT(FLAT())
LIFETIME(1);


SELECT dictGetString('dictdb.dict', 'value_default', toUInt64(12));
SELECT dictGetString('dictdb.dict', 'value_default', toUInt64(14));

SELECT dictGetString('dictdb.dict', 'value_expression', toUInt64(12));
SELECT dictGetString('dictdb.dict', 'value_expression', toUInt64(14));

DROP DATABASE IF EXISTS dictdb;
