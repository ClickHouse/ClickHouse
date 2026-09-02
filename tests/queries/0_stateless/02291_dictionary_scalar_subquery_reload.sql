DROP TABLE IF EXISTS test_dictionary_source_table;
CREATE TABLE test_dictionary_source_table
(
    id UInt64,
    value String
) ENGINE = TinyLog;

DROP TABLE IF EXISTS test_dictionary_view;
CREATE VIEW test_dictionary_view
(
    id UInt64,
    value String
) AS SELECT id, value FROM test_dictionary_source_table WHERE id = (SELECT max(id) FROM test_dictionary_source_table);

INSERT INTO test_dictionary_source_table VALUES (1, '1'), (2, '2'), (3, '3');

DROP DICTIONARY IF EXISTS test_dictionary;
CREATE DICTIONARY test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_dictionary_view'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT());

SELECT * FROM test_dictionary;

INSERT INTO test_dictionary_source_table VALUES (4, '4');
SYSTEM RELOAD DICTIONARY test_dictionary;

SELECT * FROM test_dictionary;

DROP DICTIONARY test_dictionary;
DROP VIEW test_dictionary_view;
DROP TABLE test_dictionary_source_table;
