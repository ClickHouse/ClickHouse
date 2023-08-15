
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.dict_data (key UInt64, val UInt64) Engine=Memory();
CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict
(
  key UInt64 DEFAULT 0,
  val UInt64 DEFAULT 10
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dict_data' PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 0 MAX 0)
LAYOUT(FLAT());

SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
SELECT dictGetUInt64({CLICKHOUSE_DATABASE:String}||'.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';

SELECT 'SYSTEM RELOAD DICTIONARY';
SYSTEM RELOAD DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict;
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';
SELECT dictGetUInt64({CLICKHOUSE_DATABASE:String}||'.dict', 'val', toUInt64(0));
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';

SELECT 'CREATE DATABASE';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT query_count FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict';

DROP DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.dict;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.dict_data;
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
