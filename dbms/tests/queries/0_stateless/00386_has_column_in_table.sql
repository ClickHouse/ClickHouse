CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.has_column_in_table;
CREATE TABLE test.has_column_in_table (i Int64, s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;

/* existing column */
SELECT hasColumnInTable('test', 'has_column_in_table', 'i');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'i');
SELECT hasColumnInTable('test', 'has_column_in_table', 's');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 's');
SELECT hasColumnInTable('test', 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable('test', 'has_column_in_table', 'nest.y');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'nest.y');

/* not existing column */
SELECT hasColumnInTable('test', 'has_column_in_table', 'nest');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'nest');
SELECT hasColumnInTable('test', 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable('test', 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('localhost', 'test', 'has_column_in_table', 'not_existing');

DROP TABLE test.has_column_in_table;
