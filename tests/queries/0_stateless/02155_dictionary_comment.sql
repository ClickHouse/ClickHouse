DROP TABLE IF EXISTS _02155_test_table;
CREATE TABLE _02155_test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO _02155_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS _02155_test_dictionary;
CREATE DICTIONARY _02155_test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '_02155_test_table'))
LAYOUT(DIRECT());

SELECT name, comment FROM system.dictionaries WHERE name == '_02155_test_dictionary' AND database == currentDatabase();

ALTER TABLE _02155_test_dictionary COMMENT COLUMN value 'value_column'; --{serverError 48}

ALTER TABLE _02155_test_dictionary MODIFY COMMENT '_02155_test_dictionary_comment_0';
SELECT name, comment FROM system.dictionaries WHERE name == '_02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '_02155_test_dictionary' AND database == currentDatabase();

SELECT * FROM _02155_test_dictionary;
SELECT name, comment FROM system.dictionaries WHERE name == '_02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '_02155_test_dictionary' AND database == currentDatabase();

ALTER TABLE _02155_test_dictionary MODIFY COMMENT '_02155_test_dictionary_comment_1';
SELECT name, comment FROM system.dictionaries WHERE name == '_02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '_02155_test_dictionary' AND database == currentDatabase();

DROP TABLE IF EXISTS _02155_test_dictionary_view;
CREATE TABLE _02155_test_dictionary_view
(
    id UInt64,
    value String
) ENGINE=Dictionary(concat(currentDatabase(), '._02155_test_dictionary'));

SELECT * FROM _02155_test_dictionary_view;

ALTER TABLE _02155_test_dictionary_view COMMENT COLUMN value 'value_column'; --{serverError 48}

ALTER TABLE _02155_test_dictionary_view MODIFY COMMENT '_02155_test_dictionary_view_comment_0';
SELECT name, comment FROM system.tables WHERE name == '_02155_test_dictionary_view' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '_02155_test_dictionary_view' AND database == currentDatabase();

DROP TABLE _02155_test_dictionary_view;
DROP TABLE _02155_test_table;
DROP DICTIONARY _02155_test_dictionary;
