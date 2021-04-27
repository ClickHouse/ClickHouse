DROP DATABASE IF EXISTS dict_db_01036;
CREATE DATABASE dict_db_01036;

CREATE TABLE dict_db_01036.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY dict_db_01036.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'dict_db_01036'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_01036' AND name = 'dict';
SELECT dictGetUInt64('dict_db_01036.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_01036' AND name = 'dict';

SELECT 'SYSTEM RELOAD DICTIONARY';
SYSTEM RELOAD DICTIONARY dict_db_01036.dict;
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_01036' AND name = 'dict';
SELECT dictGetUInt64('dict_db_01036.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_01036' AND name = 'dict';

SELECT 'CREATE DATABASE';
DROP DATABASE IF EXISTS empty_db_01036;
CREATE DATABASE empty_db_01036;
SELECT query_count FROM system.dictionaries WHERE database = 'dict_db_01036' AND name = 'dict';

DROP DICTIONARY dict_db_01036.dict;
DROP TABLE dict_db_01036.dict_data;
DROP DATABASE dict_db_01036;
DROP DATABASE empty_db_01036;
