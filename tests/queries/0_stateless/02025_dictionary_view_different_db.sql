-- Tags: no-parallel

DROP DATABASE IF EXISTS _2025_test_db;
CREATE DATABASE _2025_test_db;

DROP TABLE IF EXISTS _2025_test_db.test_table;
CREATE TABLE _2025_test_db.test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO _2025_test_db.test_table VALUES (0, 'Value');

CREATE DICTIONARY _2025_test_db.test_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
LAYOUT(DIRECT())
SOURCE(CLICKHOUSE(TABLE 'test_table' DB '_2025_test_db'));

DROP TABLE IF EXISTS _2025_test_db.view_table;
CREATE TABLE _2025_test_db.view_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO _2025_test_db.view_table VALUES (0, 'ViewValue');

DROP VIEW IF EXISTS test_view_different_db;
CREATE VIEW test_view_different_db AS SELECT id, value, dictGet('_2025_test_db.test_dictionary', 'value', id) FROM _2025_test_db.view_table;
SELECT * FROM test_view_different_db;

DROP TABLE _2025_test_db.test_table;
DROP DICTIONARY _2025_test_db.test_dictionary;
DROP TABLE _2025_test_db.view_table;

DROP VIEW test_view_different_db;

DROP DATABASE _2025_test_db;
