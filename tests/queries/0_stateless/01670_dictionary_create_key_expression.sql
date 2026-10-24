-- Tags: no-parallel

CREATE DATABASE database_dictionary_test_key_expression;

CREATE TABLE database_dictionary_test_key_expression.test_for_dictionary (value String) ENGINE=TinyLog;
INSERT INTO database_dictionary_test_key_expression.test_for_dictionary VALUES ('Test1'), ('Test2'), ('Test3');

SELECT 'Simple';

CREATE DICTIONARY database_dictionary_test_key_expression.test_query_log_dictionary_simple
(
    `value_id` UInt64 EXPRESSION cityHash64(value),
    `value` String
)
PRIMARY KEY value_id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'test_for_dictionary' DB 'database_dictionary_test_key_expression'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED());

SELECT * FROM database_dictionary_test_key_expression.test_query_log_dictionary_simple ORDER BY value_id;

DROP DICTIONARY IF EXISTS database_dictionary_test_key_expression.test_query_log_dictionary_simple;

SELECT 'Complex';

CREATE DICTIONARY database_dictionary_test_key_expression.test_query_log_dictionary_complex
(
    `value_id` UInt64 EXPRESSION cityHash64(value),
    `value_length` UInt64 EXPRESSION length(value),
    `value` String
)
PRIMARY KEY value_id, value_length
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'test_for_dictionary' DB 'database_dictionary_test_key_expression'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(COMPLEX_KEY_HASHED());

SELECT * FROM database_dictionary_test_key_expression.test_query_log_dictionary_complex ORDER BY value_id;

DROP DICTIONARY IF EXISTS database_dictionary_test_key_expression.test_query_log_dictionary_complex;

DROP TABLE IF EXISTS database_dictionary_test_key_expression.test_for_dictionary;
DROP DATABASE IF EXISTS database_dictionary_test_key_expression;
