DROP TABLE IF EXISTS 02155_test_table;
CREATE TABLE 02155_test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO 02155_test_table VALUES (0, 'Value');

DROP DICTIONARY IF EXISTS 02155_test_dictionary;
CREATE DICTIONARY 02155_test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02155_test_table'))
LAYOUT(DIRECT());

SELECT name, comment FROM system.dictionaries WHERE name == '02155_test_dictionary' AND database == currentDatabase();

ALTER TABLE 02155_test_dictionary COMMENT COLUMN value 'value_column'; --{serverError 48}

ALTER TABLE 02155_test_dictionary MODIFY COMMENT '02155_test_dictionary_comment_0';
SELECT name, comment FROM system.dictionaries WHERE name == '02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '02155_test_dictionary' AND database == currentDatabase();

SELECT * FROM 02155_test_dictionary;
SELECT name, comment FROM system.dictionaries WHERE name == '02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '02155_test_dictionary' AND database == currentDatabase();

ALTER TABLE 02155_test_dictionary MODIFY COMMENT '02155_test_dictionary_comment_1';
SELECT name, comment FROM system.dictionaries WHERE name == '02155_test_dictionary' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '02155_test_dictionary' AND database == currentDatabase();

DROP TABLE IF EXISTS 02155_test_dictionary_view;
CREATE TABLE 02155_test_dictionary_view
(
    id UInt64,
    value String
) ENGINE=Dictionary(concat(currentDatabase(), '.02155_test_dictionary'));

SELECT * FROM 02155_test_dictionary_view;

ALTER TABLE 02155_test_dictionary_view COMMENT COLUMN value 'value_column'; --{serverError 48}

ALTER TABLE 02155_test_dictionary_view MODIFY COMMENT '02155_test_dictionary_view_comment_0';
SELECT name, comment FROM system.tables WHERE name == '02155_test_dictionary_view' AND database == currentDatabase();
SELECT name, comment FROM system.tables WHERE name == '02155_test_dictionary_view' AND database == currentDatabase();

DROP TABLE 02155_test_dictionary_view;
DROP TABLE 02155_test_table;
DROP DICTIONARY 02155_test_dictionary;
