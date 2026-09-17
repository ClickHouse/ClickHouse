-- `Set` keeps no settings object: its creator applies the `SETTINGS` clause to a `SetSettings`, uses `disk` and
-- `persistent`, and discards the rest. Those two are the only settings the engine acts on, and the table keeps
-- both, so `system.table_settings` reports them with the values the table holds. Unstated, they are the struct's
-- fixed defaults, so unlike `Join`'s they do not depend on the server.

DROP TABLE IF EXISTS set_plain;
DROP TABLE IF EXISTS set_stated;

CREATE TABLE set_plain (k UInt64) ENGINE = Set;

-- `input_format_tsv_skip_first_lines` is one of the format settings `SetSettings` declares and the engine never
-- reads. It is accepted and kept in the definition, but not reported: a value for it would describe nothing the
-- table does.
CREATE TABLE set_stated (k UInt64) ENGINE = Set
    SETTINGS persistent = 0, disk = 'default', input_format_tsv_skip_first_lines = 2;

SELECT '-- nothing stated: both settings at their defaults';
SELECT name, value, `default`, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'set_plain'
ORDER BY name;

SELECT '-- stated: reported as the definition, and nothing else is';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'set_stated'
ORDER BY name;

SELECT '-- the unused format setting still stays in the definition';
SELECT create_table_query LIKE '%input_format_tsv_skip_first_lines = 2%' FROM system.tables
WHERE database = currentDatabase() AND name = 'set_stated';

SELECT '-- the rows agree with what `system.engine_settings` says of the engine';
SELECT count() FROM (
    SELECT name, `default`, description, type, tier FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'set_plain'
    EXCEPT
    SELECT name, `default`, description, type, tier FROM system.engine_settings WHERE engine_name = 'Set');

SELECT '-- `persistent` is the value the engine acts on';
DROP TABLE IF EXISTS set_volatile;
CREATE TABLE set_volatile (k UInt64) ENGINE = Set SETTINGS persistent = 0;
INSERT INTO set_volatile VALUES (1), (2);
DETACH TABLE set_volatile;
ATTACH TABLE set_volatile;
SELECT count() FROM set_volatile;

SELECT '-- a temporary table reports its definition too';
CREATE TEMPORARY TABLE set_temporary (k UInt64) ENGINE = Set SETTINGS persistent = 0;
SELECT name, value, source FROM system.table_settings
WHERE database = '' AND table = 'set_temporary' AND name = 'persistent';

DROP TABLE set_plain;
DROP TABLE set_stated;
DROP TABLE set_volatile;
