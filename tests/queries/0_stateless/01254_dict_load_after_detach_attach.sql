DROP DATABASE IF EXISTS dict_db_01254;
CREATE DATABASE dict_db_01254;

CREATE TABLE dict_db_01254.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict_db_01254.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_01254'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

DETACH DATABASE dict_db_01254;
ATTACH DATABASE dict_db_01254;

SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';
SYSTEM RELOAD DICTIONARY dict_db_01254.dict;
SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';
SELECT dictGetUInt64('dict_db_01254.dict', 'val', toUInt64(0));
SELECT query_count, status FROM system.dictionaries WHERE database = 'dict_db_01254' AND name = 'dict';

DROP DATABASE dict_db_01254;
