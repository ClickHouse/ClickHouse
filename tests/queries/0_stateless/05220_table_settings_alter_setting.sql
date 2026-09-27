-- `ALTER ... MODIFY SETTING` and `RESET SETTING` rewrite the table's stored `CREATE` query, so what they
-- change is reported as `definition` afterwards and what they reset stops being reported as one. A
-- setting an `ALTER` wrote and a setting the `CREATE` stated are deliberately indistinguishable - the
-- description of the `source` column says so - and this pins that they are reported at all.

DROP TABLE IF EXISTS t_alter_setting_reporting;

CREATE TABLE t_alter_setting_reporting (a UInt64) ENGINE = MergeTree ORDER BY a;

SELECT '-- a setting no one stated is not attributed to the definition';
-- `min_rows_for_wide_part` because the harness does not randomize it into the `CREATE` query, which would
-- state it in the definition before the `ALTER` this test is about ever runs. Not `= 'default'`: a server whose
-- `<merge_tree>` configuration section sets it reports `config`. Both are named, rather than accepting anything
-- that is not `definition`, so the assertion still fails if the value starts claiming a source it cannot have.
SELECT name, source IN ('default', 'config') AS not_stated FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_rows_for_wide_part';

SELECT '-- MODIFY SETTING reports the value it wrote, as definition';
ALTER TABLE t_alter_setting_reporting MODIFY SETTING min_rows_for_wide_part = 12345;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_rows_for_wide_part';

SELECT '-- which is the query the table now stores';
SELECT create_table_query LIKE '%min_rows_for_wide_part = 12345%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_alter_setting_reporting';

SELECT '-- RESET SETTING takes it back out of the definition';
ALTER TABLE t_alter_setting_reporting RESET SETTING min_rows_for_wide_part;
SELECT name, source IN ('default', 'config') AS not_stated FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_rows_for_wide_part';

SELECT create_table_query LIKE '%min_rows_for_wide_part%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_alter_setting_reporting';

DROP TABLE t_alter_setting_reporting;
