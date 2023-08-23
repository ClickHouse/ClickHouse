-- Tags: no-parallel

SET allow_experimental_analyzer = 1;

DROP DATABASE IF EXISTS 02563_db;
CREATE DATABASE 02563_db;

DROP TABLE IF EXISTS 02563_db.test_merge_table_1;
CREATE TABLE 02563_db.test_merge_table_1
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO 02563_db.test_merge_table_1 VALUES (0, 'Value_0');

DROP TABLE IF EXISTS 02563_db.test_merge_table_2;
CREATE TABLE 02563_db.test_merge_table_2
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO 02563_db.test_merge_table_2 VALUES (1, 'Value_1');

DROP TABLE IF EXISTS 02563_db.test_merge_table;
CREATE TABLE 02563_db.test_merge_table
(
    id UInt64,
    value String
) ENGINE=Merge(02563_db, '^test_merge_table');

SELECT id, value, _database, _table FROM 02563_db.test_merge_table ORDER BY id;

DROP TABLE 02563_db.test_merge_table;
DROP TABLE 02563_db.test_merge_table_1;
DROP TABLE 02563_db.test_merge_table_2;
DROP DATABASE 02563_db;
