-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102705
-- `SELECT ... FROM system.detached_tables WHERE uuid = <...>` used to throw
-- `LOGICAL_ERROR: Expected the argument №1 ('uuid' of type UUID) to have 1 rows, but it has 0`
-- because `getFilteredTables` created `uuid_column` for the filter but never populated it
-- in the detached path, while `table_column` was filled, producing mismatched column lengths.

DROP TABLE IF EXISTS t_detached_uuid_filter SYNC;
CREATE TABLE t_detached_uuid_filter (key Int) ENGINE = MergeTree ORDER BY key;

-- The query below would raise a LOGICAL_ERROR before the fix.
SELECT count() FROM system.detached_tables
WHERE database = currentDatabase()
  AND uuid IN (SELECT uuid FROM system.tables
               WHERE database = currentDatabase()
                 AND name = 't_detached_uuid_filter');

DETACH TABLE t_detached_uuid_filter;

-- After DETACH we must be able to look up the entry by uuid.
SELECT count() FROM system.detached_tables
WHERE database = currentDatabase()
  AND uuid != toUUIDOrDefault('00000000-0000-0000-0000-000000000000');

-- uuid in WHERE without database also exercises the buggy code path.
SELECT count() > 0 FROM system.detached_tables
WHERE uuid != toUUIDOrDefault('00000000-0000-0000-0000-000000000000');

ATTACH TABLE t_detached_uuid_filter;
DROP TABLE t_detached_uuid_filter SYNC;
