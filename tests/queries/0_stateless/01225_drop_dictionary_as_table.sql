DROP DATABASE IF EXISTS dict_db_01225;
CREATE DATABASE dict_db_01225;

CREATE TABLE dict_db_01225.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict_db_01225.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_01225'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SYSTEM RELOAD DICTIONARY dict_db_01225.dict;

DROP TABLE dict_db_01225.dict; -- { serverError 520; }
DROP DICTIONARY dict_db_01225.dict;

DROP DATABASE dict_db_01225;
