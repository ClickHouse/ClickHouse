-- The `QueryRunner` engine consumes its settings at construction and keeps only one of them, so it used to
-- report nothing at all. It now keeps the enumeration made there, which is a complete answer: the engine
-- reads its settings from the definition alone, so whatever the definition does not state is at its default.

DROP TABLE IF EXISTS qr;

CREATE TABLE qr (query String, database String, settings Map(String, String))
ENGINE = QueryRunner SETTINGS threads = 2;

SELECT '-- what the definition states, and what it leaves at the default';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'qr' AND name IN ('threads', 'max_queue_size')
ORDER BY name;

SELECT '-- every setting the engine takes is reported, not only the one the definition states';
SELECT
    (SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 'qr')
    = (SELECT count() FROM system.engine_settings WHERE engine_name = 'QueryRunner');

DROP TABLE qr;
