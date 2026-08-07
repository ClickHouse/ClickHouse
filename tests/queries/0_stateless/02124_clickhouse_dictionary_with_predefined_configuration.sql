DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS s;
CREATE TABLE s
(
   id UInt64,
   value String
)
ENGINE = Memory;

INSERT INTO s VALUES(1, 'OK');

CREATE DICTIONARY dict
(
   id UInt64,
   value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(NAME clickhouse_dictionary PORT tcpPort() DB currentDatabase()))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(CACHE(SIZE_IN_CELLS 10));

SELECT dictGet('dict', 'value', toUInt64(1));

DROP DICTIONARY dict;
DROP TABLE s;
