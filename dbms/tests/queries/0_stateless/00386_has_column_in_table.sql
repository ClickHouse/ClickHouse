DROP TABLE IF EXISTS has_column_in_table_test;
CREATE TABLE has_column_in_table_test (i Int64, s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;

SELECT hasColumnInTable('default', 'has_column_in_table_test', 'i');
SELECT hasColumnInTable('default', 'has_column_in_table_test', 's');
SELECT hasColumnInTable('default', 'has_column_in_table_test', 'nest.x');
SELECT hasColumnInTable('default', 'has_column_in_table_test', 'nest.y');

SELECT hasColumnInTable('default', 'has_column_in_table_test', 'nest');
SELECT hasColumnInTable('default', 'has_column_in_table_test', 'nest.not_existing');
SELECT hasColumnInTable('default', 'has_column_in_table_test', 'not_existing');
