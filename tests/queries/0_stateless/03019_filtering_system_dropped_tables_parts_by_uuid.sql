-- Tags: no-ordinary-database
-- DatabaseOrdinary doesn't use delayed deletion, so `system.dropped_tables_parts` would be empty

SET database_atomic_wait_for_drop_and_detach_synchronously = 0;

CREATE TABLE mt (x Int64) ENGINE = MergeTree ORDER BY x;

INSERT INTO mt SELECT number AS x FROM numbers(5);

DROP TABLE mt;

SELECT 'without_uuid_filter', database, table, name FROM system.dropped_tables_parts WHERE database = currentDatabase();
SELECT 'with_uuid_filter', database, table, name FROM system.dropped_tables_parts WHERE database = currentDatabase() AND uuid = '00000000-0000-0000-0000-000000000000';

CREATE TABLE mt_with_uuid (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS assign_part_uuids = 1;

INSERT INTO mt_with_uuid SELECT number AS x FROM numbers(5);

CREATE TABLE uuid_store (uuid UUID) ENGINE = Memory;
INSERT INTO uuid_store SELECT uuid FROM system.parts WHERE database = currentDatabase() AND table = 'mt_with_uuid';

INSERT INTO mt_with_uuid SELECT number AS x FROM numbers(5);

DROP TABLE mt_with_uuid;

-- Avoid querying UUID as it changes from run to run
SELECT 'without_uuid_filter', database, table, name FROM system.dropped_tables_parts WHERE database = currentDatabase() AND table = 'mt_with_uuid' ORDER BY name;
SELECT 'with_uuid_filter', database, table, name FROM system.dropped_tables_parts WHERE uuid IN (SELECT uuid FROM uuid_store);
