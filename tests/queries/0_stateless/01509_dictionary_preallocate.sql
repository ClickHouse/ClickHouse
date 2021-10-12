-- The test itself does not test does preallocation works
-- It simply check SPARSE_HASHED dictionary with bunch of dictGet()
-- (since at the moment of writing there were no such test)

DROP DATABASE IF EXISTS db_01509;
CREATE DATABASE db_01509;

CREATE TABLE db_01509.data
(
  key   UInt64,
  value String
)
ENGINE = MergeTree()
ORDER BY key;
INSERT INTO db_01509.data SELECT number key, toString(number) value FROM numbers(1000);

DROP DICTIONARY IF EXISTS db_01509.dict;
CREATE DICTIONARY db_01509.dict
(
  key   UInt64,
  value String DEFAULT '-'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'data' PASSWORD '' DB 'db_01509'))
LAYOUT(SPARSE_HASHED())
LIFETIME(0);

SHOW CREATE DICTIONARY db_01509.dict;

SYSTEM RELOAD DICTIONARY db_01509.dict;

SELECT dictGet('db_01509.dict', 'value', toUInt64(1e12));
SELECT dictGet('db_01509.dict', 'value', toUInt64(0));
SELECT count() FROM db_01509.dict;

DROP DATABASE IF EXISTS db_01509;
