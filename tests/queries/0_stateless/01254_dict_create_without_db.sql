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

SELECT query_count, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
SYSTEM RELOAD DICTIONARY dict;
SELECT query_count, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
SELECT dictGetUInt64('dict', 'val', toUInt64(0));
SELECT query_count, status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
