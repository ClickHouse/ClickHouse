-- Tags: no-fasttest
-- Tag no-fasttest: the YTsaurus engine is an optional build.
--
-- `YTsaurus` records the table's own `SETTINGS` clause in the settings object as it applies it, so
-- `system.table_settings` reads the source from there rather than from the stored `CREATE` query. The two have
-- to agree on `CREATE` and after the table is loaded again from what it stored.
--
-- Nothing connects at `CREATE`, so an unreachable proxy is fine: the table is never read.

SET allow_experimental_ytsaurus_table_engine = 1;

DROP TABLE IF EXISTS ytsaurus_definition;

CREATE TABLE ytsaurus_definition (a Int64)
ENGINE = YTsaurus('http://unreachable.invalid:80', '//tmp/t', 'token')
SETTINGS encode_utf8 = 0, max_streams = 2;

SELECT '-- CREATE';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ytsaurus_definition' AND name IN ('encode_utf8', 'max_streams', 'use_lock')
ORDER BY name;

DETACH TABLE ytsaurus_definition;
ATTACH TABLE ytsaurus_definition;

SELECT '-- loaded again from what it stored';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ytsaurus_definition' AND name IN ('encode_utf8', 'max_streams', 'use_lock')
ORDER BY name;

DROP TABLE ytsaurus_definition;
