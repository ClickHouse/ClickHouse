-- The valueless `name` shorthand means `name = true`, so it only makes sense for a Bool setting.
-- Column-level settings are consumed as a raw `SettingsChanges`, where nothing knows the settings
-- schema and could reject a Bool afterwards - and there is nothing to allow anyway, because every
-- setting permitted at column level is a number. So the shorthand is not accepted there at all.

DROP TABLE IF EXISTS t_valueless_column_setting;

SELECT '-- The shorthand is rejected in a column declaration';
CREATE TABLE t_valueless_column_setting (x UInt64 SETTINGS (min_compress_block_size)) ENGINE = MergeTree ORDER BY x; -- { error SYNTAX_ERROR }
SELECT 'ok';

SELECT '-- A column-level setting with a value still works';
CREATE TABLE t_valueless_column_setting (x UInt64 SETTINGS (min_compress_block_size = 1024)) ENGINE = MergeTree ORDER BY x;
SELECT 'ok';

SELECT '-- And it is rejected in ALTER ... MODIFY COLUMN as well';
ALTER TABLE t_valueless_column_setting MODIFY COLUMN x UInt64 SETTINGS (min_compress_block_size); -- { error SYNTAX_ERROR }
SELECT 'ok';

DROP TABLE t_valueless_column_setting;
