-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dictionary_source_table
(
	key UInt8,
    value String
)
ENGINE = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.dictionary_source_table VALUES (1, 'First');

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dictionary
(
    key UInt64,
    value String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'dictionary_source_table' HOST hostName() PORT tcpPort()))
LIFETIME(0)
LAYOUT(FLAT());

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.dictionary;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dictionary;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dictionary_source_table;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
