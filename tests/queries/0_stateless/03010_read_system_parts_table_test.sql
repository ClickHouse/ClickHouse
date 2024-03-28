-- Tags: no-shared-merge-tree
-- The parts might named differently with SMT
DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree ORDER BY uid PARTITION BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);

SELECT uuid, name from system.parts WHERE database = currentDatabase() AND table = 'users';

SELECT uuid, name, table from system.parts WHERE database = currentDatabase() AND table = 'users' AND uuid = '00000000-0000-0000-0000-000000000000';
SELECT uuid, name, table, column from system.parts_columns WHERE database = currentDatabase() AND table = 'users' AND uuid = '00000000-0000-0000-0000-000000000000';
DROP TABLE IF EXISTS users;

CREATE TABLE mt_with_uuid (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS assign_part_uuids = 1;

INSERT INTO mt_with_uuid SELECT number AS x FROM numbers(5);

CREATE TABLE uuid_store (uuid UUID) ENGINE = Memory;
INSERT INTO uuid_store SELECT uuid FROM system.parts WHERE database = currentDatabase() AND table = 'mt_with_uuid';

INSERT INTO mt_with_uuid SELECT number AS x FROM numbers(5);

-- Avoid querying UUID as it changes from run to run
SELECT 'without_uuid_filter', name, table FROM system.parts WHERE database = currentDatabase() AND table = 'mt_with_uuid' ORDER BY name;
SELECT 'with_uuid_filter', name, table FROM system.parts WHERE uuid IN (SELECT uuid FROM uuid_store);
SELECT 'without_uuid_filter', name, table, column FROM system.parts_columns WHERE database = currentDatabase() AND table = 'mt_with_uuid' ORDER BY name;
SELECT 'with_uuid_filter', name, table, column FROM system.parts_columns WHERE uuid IN (SELECT uuid FROM uuid_store);
