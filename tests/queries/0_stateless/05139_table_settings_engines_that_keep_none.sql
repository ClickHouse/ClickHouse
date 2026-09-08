-- An engine that consumes its settings at construction and keeps none of them cannot say what its
-- settings are. The base implementation could report what the `CREATE` query states, but for these
-- engines that is not the whole answer - the effective values also come from the query context, a
-- named collection or a connection pool default - and a partial answer that looks complete is worse
-- than an admitted gap. So they report nothing, and `system.table_settings` says which they are.
--
-- Reporting them properly is separate work. This test pins the gap so that closing it is a visible
-- change rather than a silent one.

DROP TABLE IF EXISTS kept_none;
DROP TABLE IF EXISTS kept_some;

SELECT '-- an engine that keeps nothing reports nothing, even when its definition states a setting';
CREATE TABLE kept_none (a UInt64) ENGINE = Set SETTINGS persistent = 0;
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 'kept_none';

SELECT '-- while `system.engine_settings` still advertises the engine, which is the gap';
SELECT count() > 0 FROM system.engine_settings WHERE engine_name = 'Set';

SELECT '-- an engine that keeps its settings is unaffected';
CREATE TABLE kept_some (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096;
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kept_some' AND name = 'index_granularity';

SELECT '-- and an engine that keeps none but is not advertised still reports its definition';
-- `URL` is in neither `system.engine_settings` nor the list above, so there is no inconsistency to
-- resolve for it and the rows it does report are accurate.
CREATE TABLE not_advertised (a String) ENGINE = URL('http://localhost:1/', CSV) SETTINGS url_base = 'http://host/';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'not_advertised';

DROP TABLE kept_none;
DROP TABLE kept_some;
DROP TABLE not_advertised;
