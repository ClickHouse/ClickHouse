-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dictionary_source_table
(
   key UInt64,
   value1 UInt64,
   value2 UInt64
)
ENGINE = TinyLog;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.dictionary_source_table VALUES (0, 2, 3), (1, 5, 6), (2, 8, 9);

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary(key UInt64, value1 UInt64, value1 UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB currentDatabase()))
LAYOUT(COMPLEX_KEY_DIRECT()); -- {serverError BAD_ARGUMENTS}

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.test_dictionary(key UInt64, value1 UInt64, value2 UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table' DB currentDatabase()))
LAYOUT(COMPLEX_KEY_DIRECT());

SELECT number, dictGet('test_dictionary', 'value1', tuple(number)) as value1,
   dictGet('test_dictionary', 'value2', tuple(number)) as value2 FROM system.numbers LIMIT 3;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
