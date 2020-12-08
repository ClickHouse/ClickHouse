DROP DATABASE IF EXISTS `foo 1234`;
CREATE DATABASE `foo 1234`;

CREATE TABLE `foo 1234`.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY `foo 1234`.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB 'foo 1234'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT query_count FROM system.dictionaries WHERE database = 'foo 1234' AND name = 'dict';
SELECT dictGetUInt64('foo 1234.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = 'foo 1234' AND name = 'dict';

SELECT 'SYSTEM RELOAD DICTIONARY';
SYSTEM RELOAD DICTIONARY `foo 1234`.dict;
SELECT query_count FROM system.dictionaries WHERE database = 'foo 1234' AND name = 'dict';
SELECT dictGetUInt64('foo 1234.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = 'foo 1234' AND name = 'dict';

SELECT 'CREATE DATABASE';
DROP DATABASE IF EXISTS `foo 123`;
CREATE DATABASE `foo 123`;
SELECT query_count FROM system.dictionaries WHERE database = 'foo 1234' AND name = 'dict';

DROP DICTIONARY `foo 1234`.dict;
DROP TABLE `foo 1234`.dict_data;
DROP DATABASE `foo 1234`;
DROP DATABASE `foo 123`;
