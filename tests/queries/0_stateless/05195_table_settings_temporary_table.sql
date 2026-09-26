-- A session's temporary tables are reported with an empty `database`, the way `system.columns` reports
-- them, and a filter on a database leaves them out.

DROP TEMPORARY TABLE IF EXISTS tmp_settings;
CREATE TEMPORARY TABLE tmp_settings (a UInt64) ENGINE = Memory SETTINGS min_rows_to_keep = 7, max_rows_to_keep = 70;

SELECT '-- its settings, with an empty database';
SELECT database, table, name, value, source FROM system.table_settings
WHERE database = '' AND table = 'tmp_settings' AND name IN ('min_rows_to_keep', 'max_rows_to_keep')
ORDER BY name;

SELECT '-- a filter on the current database leaves them out';
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 'tmp_settings';

-- The rows are the same whether or not `system.table_settings` filters the temporary tables before reading
-- their settings (`92a5af36f0b`): what changes is the work done, which no assertion here can see. This case
-- pins the result of a predicate naming another table; it cannot catch the filter keeping too much.
SELECT '-- and so does a filter naming another table';
SELECT count() FROM system.table_settings WHERE database = '' AND table = 'tmp_settings_other';

DROP TEMPORARY TABLE tmp_settings;
