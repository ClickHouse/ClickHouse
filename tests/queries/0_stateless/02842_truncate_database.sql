-- Tags: no-parallel

DROP DATABASE IF EXISTS test_truncate_database;

-- test TRUNCATE DATABASE operation.
-- create tables, views and dictionary and populate them. Then try truncating the database.
-- all tables, views and dictionaries should be removed leaving an empty database
CREATE DATABASE test_truncate_database;
USE test_truncate_database;

-- create tables with several different types of table engines
CREATE TABLE source_table_memory (x UInt16) ENGINE = Memory;
CREATE TABLE source_table_log (x UInt16) ENGINE = Log;
CREATE TABLE source_table_tiny_log (x UInt16) ENGINE = TinyLog;
CREATE TABLE source_table_stripe_log (x UInt16) ENGINE = StripeLog;
CREATE TABLE source_table_merge_tree (x UInt16) ENGINE = MergeTree ORDER BY x PARTITION BY x;
-- create dictionary source table
CREATE TABLE source_table_dictionary
(
    id UInt64,
    value String
) ENGINE = Memory();

-- insert data into the tables
INSERT INTO source_table_memory SELECT * FROM system.numbers LIMIT 10;
INSERT INTO source_table_log SELECT * FROM system.numbers LIMIT 10;
INSERT INTO source_table_tiny_log SELECT * FROM system.numbers LIMIT 10;
INSERT INTO source_table_stripe_log SELECT * FROM system.numbers LIMIT 10;
INSERT INTO source_table_merge_tree SELECT * FROM system.numbers LIMIT 10;
INSERT INTO source_table_dictionary VALUES (1, 'First');


-- create view based on the tables
CREATE VIEW dest_view_memory (x UInt64) AS SELECT * FROM source_table_memory;
CREATE VIEW dest_view_log (x UInt64) AS SELECT * FROM source_table_log;
CREATE VIEW dest_view_tiny_log (x UInt64) AS SELECT * FROM source_table_tiny_log;
CREATE VIEW dest_view_stripe_log (x UInt64) AS SELECT * FROM source_table_stripe_log;
CREATE VIEW dest_view_merge_tree (x UInt64) AS SELECT * FROM source_table_merge_tree;
-- create dictionary based on source table
CREATE DICTIONARY dest_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB 'test_truncate_database' TABLE 'source_table_dictionary'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);


SELECT * FROM dest_view_memory ORDER BY x LIMIT 1;
SELECT * FROM dest_view_log ORDER BY x LIMIT 1;
SELECT * FROM dest_view_tiny_log ORDER BY x LIMIT 1;
SELECT * FROM dest_view_stripe_log ORDER BY x LIMIT 1;
SELECT * FROM dest_view_merge_tree ORDER BY x LIMIT 1;
SELECT name, database, element_count FROM system.dictionaries WHERE database = 'test_truncate_database' AND name = 'dest_dictionary';
SELECT * FROM dest_dictionary;
SELECT '=== TABLES IN test_truncate_database ===';
SHOW TABLES FROM test_truncate_database;
SELECT '=== DICTIONARIES IN test_truncate_database ===';
SHOW DICTIONARIES FROM test_truncate_database;

TRUNCATE DATABASE test_truncate_database;

SELECT * FROM dest_view_set ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT * FROM dest_view_memory ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT * FROM dest_view_log ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT * FROM dest_view_tiny_log ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT * FROM dest_view_stripe_log ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT * FROM dest_view_merge_tree ORDER BY x LIMIT 1; -- {serverError UNKNOWN_TABLE}
SELECT name, database, element_count FROM system.dictionaries WHERE database = 'test_truncate_database' AND name = 'dest_dictionary';
SELECT * FROM dest_dictionary; -- {serverError UNKNOWN_TABLE}
SHOW TABLES FROM test_truncate_database;
SHOW DICTIONARIES FROM test_truncate_database;

CREATE TABLE new_table (x UInt16) ENGINE = MergeTree ORDER BY x;
select 'new tables';
SHOW TABLES FROM test_truncate_database;

DROP DATABASE test_truncate_database;
