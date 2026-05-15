-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102705

DROP TABLE IF EXISTS t_detached_uuid_filter SYNC;
CREATE TABLE t_detached_uuid_filter (key Int) ENGINE = MergeTree ORDER BY key;

SELECT count() FROM system.detached_tables
WHERE database = currentDatabase()
  AND uuid IN (SELECT uuid FROM system.tables
               WHERE database = currentDatabase()
                 AND name = 't_detached_uuid_filter');

DETACH TABLE t_detached_uuid_filter;

SELECT count() FROM system.detached_tables
WHERE database = currentDatabase()
  AND uuid != toUUIDOrDefault('00000000-0000-0000-0000-000000000000');

SELECT count() > 0 FROM system.detached_tables
WHERE uuid != toUUIDOrDefault('00000000-0000-0000-0000-000000000000');

ATTACH TABLE t_detached_uuid_filter;
DROP TABLE t_detached_uuid_filter SYNC;
