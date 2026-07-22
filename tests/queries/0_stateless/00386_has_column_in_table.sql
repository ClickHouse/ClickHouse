DROP TABLE IF EXISTS has_column_in_table;
CREATE TABLE has_column_in_table (i Int64, s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;

/* existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'i');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'i');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 's');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 's');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.y');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.y');

/* not existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('system', 'one', '');

/* bad queries */
SELECT hasColumnInTable('', '', '');  -- { serverError UNKNOWN_TABLE }
SELECT hasColumnInTable('', 't', 'c');  -- { serverError UNKNOWN_DATABASE }
SELECT hasColumnInTable(currentDatabase(), '', 'c'); -- { serverError UNKNOWN_TABLE }
SELECT hasColumnInTable('d', 't', 's');  -- { serverError UNKNOWN_DATABASE }
SELECT hasColumnInTable(currentDatabase(), 't', 's');  -- { serverError UNKNOWN_TABLE }


DROP TABLE has_column_in_table;
