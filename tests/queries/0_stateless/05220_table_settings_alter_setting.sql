-- Tags: no-random-merge-tree-settings
-- Tag justification: the harness randomizes MergeTree settings into the `CREATE` query of every table it
-- creates, `min_bytes_for_wide_part` among them, which would make it stated in the definition before the
-- `ALTER` this test is about ever runs.
--
-- `ALTER ... MODIFY SETTING` and `RESET SETTING` rewrite the table's stored `CREATE` query, so what they
-- change is reported as `definition` afterwards and what they reset stops being reported as one. A
-- setting an `ALTER` wrote and a setting the `CREATE` stated are deliberately indistinguishable - the
-- description of the `source` column says so - and this pins that they are reported at all.

DROP TABLE IF EXISTS t_alter_setting_reporting;

CREATE TABLE t_alter_setting_reporting (a UInt64) ENGINE = MergeTree ORDER BY a;

SELECT '-- a setting no one stated is not attributed to the definition';
-- Not `= 'default'`: a server whose `<merge_tree>` configuration section sets this reports `config`, and
-- the stateless test configuration does exactly that (`tests/config/config.d/polymorphic_parts.xml`). Both
-- are named, rather than accepting anything that is not `definition`, so the assertion still fails if the
-- value starts claiming a source it cannot have.
SELECT name, source IN ('default', 'config') AS not_stated FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_bytes_for_wide_part';

SELECT '-- MODIFY SETTING reports the value it wrote, as definition';
ALTER TABLE t_alter_setting_reporting MODIFY SETTING min_bytes_for_wide_part = 12345;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_bytes_for_wide_part';

SELECT '-- which is the query the table now stores';
SELECT create_table_query LIKE '%min_bytes_for_wide_part = 12345%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_alter_setting_reporting';

SELECT '-- RESET SETTING takes it back out of the definition';
ALTER TABLE t_alter_setting_reporting RESET SETTING min_bytes_for_wide_part;
SELECT name, source IN ('default', 'config') AS not_stated FROM system.table_settings
WHERE database = currentDatabase() AND table = 't_alter_setting_reporting' AND name = 'min_bytes_for_wide_part';

SELECT create_table_query LIKE '%min_bytes_for_wide_part%' FROM system.tables
WHERE database = currentDatabase() AND name = 't_alter_setting_reporting';

DROP TABLE t_alter_setting_reporting;
