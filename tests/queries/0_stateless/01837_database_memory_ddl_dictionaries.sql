-- Tags: no-parallel, no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
USE {CLICKHOUSE_DATABASE_1:Identifier};

DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source
(
    id UInt64,
    value String
) ENGINE = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source VALUES (1, 'First');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source VALUES (2, 'Second');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source VALUES (3, 'Third');

DROP DICTIONARY IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_direct_dictionary;
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_direct_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB currentDatabase() TABLE 'simple_key_dictionary_source'))
LAYOUT(DIRECT());

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_direct_dictionary ORDER BY ALL;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_direct_dictionary;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.simple_key_dictionary_source;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
