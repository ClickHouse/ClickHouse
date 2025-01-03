DROP TABLE IF EXISTS table_function_dictionary_source_table;
CREATE TABLE table_function_dictionary_source_table
(
   id UInt64,
   value UInt64
)
ENGINE = TinyLog;

INSERT INTO table_function_dictionary_source_table VALUES (0, 0);
INSERT INTO table_function_dictionary_source_table VALUES (1, 1);

DROP DICTIONARY IF EXISTS table_function_dictionary_test_dictionary;
CREATE DICTIONARY table_function_dictionary_test_dictionary
(
   id UInt64,
   value UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DATABASE currentDatabase() TABLE 'table_function_dictionary_source_table'))
LAYOUT(DIRECT());

SELECT * FROM dictionary('table_function_dictionary_test_dictionary');

DROP DICTIONARY table_function_dictionary_test_dictionary;
DROP TABLE table_function_dictionary_source_table;
