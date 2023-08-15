DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS source;

CREATE TABLE source
(
  id UInt64,
  value String
)
ENGINE=Memory;

CREATE DICTIONARY dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source'))
LAYOUT(DIRECT());

SELECT 1 IN (SELECT dictGet('dict', 'value', materialize('1')));

DROP DICTIONARY dict;
DROP TABLE source;
