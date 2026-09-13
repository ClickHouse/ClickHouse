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

DROP TEMPORARY TABLE tmp_settings;
