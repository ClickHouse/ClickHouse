CREATE TABLE dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SYSTEM RELOAD DICTIONARY dict;

DROP TABLE dict; -- { serverError CANNOT_DETACH_DICTIONARY_AS_TABLE }
DROP DICTIONARY dict;
