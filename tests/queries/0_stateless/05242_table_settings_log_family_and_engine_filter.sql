-- The `Log` family reports both of its settings, with the disk each table keeps its data on, and describes them as
-- `system.engine_settings` does. A predicate on `engine` is applied before a table's settings are read, and must
-- still select exactly the rows it would select afterwards.

DROP TABLE IF EXISTS log_plain;
DROP TABLE IF EXISTS stripe_with_disk;
DROP TABLE IF EXISTS tiny_with_policy;
DROP TABLE IF EXISTS mt;
DROP VIEW IF EXISTS v;

CREATE TABLE log_plain (x UInt8) ENGINE = Log;
CREATE TABLE stripe_with_disk (x UInt8) ENGINE = StripeLog SETTINGS disk = 'default';
CREATE TABLE tiny_with_policy (x UInt8) ENGINE = TinyLog SETTINGS storage_policy = 'default';
CREATE TABLE mt (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 4096;
CREATE VIEW v AS SELECT 1;

SELECT '-- both settings of every Log family table, with the source of each';
SELECT table, name, value, changed, source FROM system.table_settings
WHERE database = currentDatabase() AND engine IN ('Log', 'StripeLog', 'TinyLog')
ORDER BY table, name;

SELECT '-- described as system.engine_settings describes them';
SELECT count() FROM system.table_settings AS t
INNER JOIN system.engine_settings AS e ON e.engine_name = t.engine AND e.name = t.name
WHERE t.database = currentDatabase() AND t.engine IN ('Log', 'StripeLog', 'TinyLog')
    AND (t.default != e.default OR t.type != e.type OR t.description != e.description OR t.tier != e.tier);

SELECT '-- and the engine rows report the compiled defaults as the defaults they are';
SELECT engine_name, name, changed, source FROM system.engine_settings
WHERE engine_name IN ('Log', 'StripeLog', 'TinyLog') ORDER BY ALL;

SELECT '-- a view has no settings of its own, and a materialized view reports through its inner table';
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1024 AS SELECT x FROM mt;
SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table IN ('v', 'mv');
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND startsWith(table, '.inner') AND name = 'index_granularity';
DROP TABLE mv;

SELECT '-- a predicate on engine selects the same rows as filtering after the fact';
SELECT
    (SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND engine = 'Log')
        = (SELECT countIf(engine = 'Log') FROM system.table_settings WHERE database = currentDatabase()),
    (SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND NOT engine LIKE '%Log')
        = (SELECT countIf(NOT engine LIKE '%Log') FROM system.table_settings WHERE database = currentDatabase()),
    (SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND (engine = 'Log' OR table = 'mt'))
        = (SELECT countIf(engine = 'Log' OR table = 'mt') FROM system.table_settings WHERE database = currentDatabase());

SELECT '-- and combined with a predicate on the table';
SELECT table, name FROM system.table_settings
WHERE database = currentDatabase() AND engine = 'MergeTree' AND table = 'mt' AND name = 'index_granularity';

DROP TABLE log_plain;
DROP TABLE stripe_with_disk;
DROP TABLE tiny_with_policy;
DROP TABLE mt;
DROP VIEW v;
