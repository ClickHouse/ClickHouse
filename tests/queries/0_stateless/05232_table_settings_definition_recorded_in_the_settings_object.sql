-- Tags: no-random-merge-tree-settings
-- Tag justification: the harness randomizes MergeTree settings into the `CREATE` query of every table it
-- creates, which would state in the definition the settings this test expects to be unstated.
--
-- `MergeTree` and `Memory` record the table's own `SETTINGS` clause in the settings object as they apply it,
-- so `system.table_settings` reads the source from there rather than from the stored `CREATE` query. The two
-- have to agree at every point the settings can change: on `CREATE`, after `ALTER ... MODIFY SETTING` and
-- `RESET SETTING`, and after the table is loaded again from what it stored. `index_granularity` is the case
-- they could disagree on: `MergeTree` writes it into the stored query when the clause leaves it out, so it is
-- the definition's from `CREATE` on, not only after the table is loaded again.
--
-- An unstated setting is checked as `default` or `config`, not one of them: the stateless test server's
-- `<merge_tree>` section sets some of these.

DROP TABLE IF EXISTS mt_definition;
DROP TABLE IF EXISTS memory_definition;

CREATE TABLE mt_definition (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 12345;

SELECT '-- MergeTree: CREATE';
SELECT name, source = 'definition' AS stated, source IN ('default', 'config') AS unstated
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt_definition'
    AND name IN ('index_granularity', 'min_bytes_for_wide_part', 'min_rows_for_wide_part')
ORDER BY name;

SELECT '-- MergeTree: MODIFY SETTING';
ALTER TABLE mt_definition MODIFY SETTING min_rows_for_wide_part = 7;
SELECT name, source = 'definition' AS stated, source IN ('default', 'config') AS unstated
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt_definition'
    AND name IN ('index_granularity', 'min_bytes_for_wide_part', 'min_rows_for_wide_part')
ORDER BY name;

SELECT '-- MergeTree: RESET SETTING';
ALTER TABLE mt_definition RESET SETTING min_bytes_for_wide_part;
SELECT name, source = 'definition' AS stated, source IN ('default', 'config') AS unstated
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt_definition'
    AND name IN ('index_granularity', 'min_bytes_for_wide_part', 'min_rows_for_wide_part')
ORDER BY name;

SELECT '-- MergeTree: loaded again from what it stored';
DETACH TABLE mt_definition;
ATTACH TABLE mt_definition;
SELECT name, source = 'definition' AS stated, source IN ('default', 'config') AS unstated
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt_definition'
    AND name IN ('index_granularity', 'min_bytes_for_wide_part', 'min_rows_for_wide_part')
ORDER BY name;

CREATE TABLE memory_definition (a UInt64) ENGINE = Memory SETTINGS max_rows_to_keep = 100;

SELECT '-- Memory: CREATE';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'memory_definition' AND name IN ('max_rows_to_keep', 'min_rows_to_keep')
ORDER BY name;

SELECT '-- Memory: MODIFY SETTING';
ALTER TABLE memory_definition MODIFY SETTING min_rows_to_keep = 10;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'memory_definition' AND name IN ('max_rows_to_keep', 'min_rows_to_keep')
ORDER BY name;

SELECT '-- Memory: loaded again from what it stored';
DETACH TABLE memory_definition;
ATTACH TABLE memory_definition;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'memory_definition' AND name IN ('max_rows_to_keep', 'min_rows_to_keep')
ORDER BY name;

DROP TABLE mt_definition;
DROP TABLE memory_definition;
