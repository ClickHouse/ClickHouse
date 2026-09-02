
SET send_logs_level = 'fatal';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.table_for_dict
(
  key_column UInt64,
  second_column UInt8,
  third_column String
)
ENGINE = MergeTree()
ORDER BY key_column;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.table_for_dict VALUES (1, 100, 'Hello world');

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict1
(
  key_column UInt64 DEFAULT 0,
  second_column UInt8 DEFAULT 1,
  third_column String DEFAULT 'qqq'
)
PRIMARY KEY key_column
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' PASSWORD '' DB currentDatabase()))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

SELECT 'INITIALIZING DICTIONARY';

SELECT dictGetUInt8({CLICKHOUSE_DATABASE_1:String}||'.dict1', 'second_column', toUInt64(100500));

SELECT lifetime_min, lifetime_max FROM system.dictionaries WHERE database={CLICKHOUSE_DATABASE_1:String} AND name = 'dict1';

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.dict1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.table_for_dict;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

