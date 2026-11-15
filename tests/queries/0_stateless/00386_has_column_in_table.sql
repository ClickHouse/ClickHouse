DROP TABLE IF EXISTS has_column_in_table;
CREATE TABLE has_column_in_table (i Int64, s String, nest Nested(x UInt8, y UInt32)) ENGINE = Memory;

/* existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'i');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 's');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.y');

/* not existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('system', 'one', '');

/* the remote-server overload (hostname[, username[, password]]) has been removed */
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'i');  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasColumnInTable('localhost', 'default', currentDatabase(), 'has_column_in_table', 'i');  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasColumnInTable('localhost', 'default', '', currentDatabase(), 'has_column_in_table', 'i');  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

/* bad queries */
SELECT hasColumnInTable('', '', '');  -- { serverError UNKNOWN_TABLE }
SELECT hasColumnInTable('', 't', 'c');  -- { serverError UNKNOWN_DATABASE }
SELECT hasColumnInTable(currentDatabase(), '', 'c'); -- { serverError UNKNOWN_TABLE }
SELECT hasColumnInTable('d', 't', 's');  -- { serverError UNKNOWN_DATABASE }
SELECT hasColumnInTable(currentDatabase(), 't', 's');  -- { serverError UNKNOWN_TABLE }


DROP TABLE has_column_in_table;
