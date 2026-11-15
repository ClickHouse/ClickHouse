-- Tags: no-shared-merge-tree
SET alter_sync = 2;
-- {echoOn}
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (1), (2), (3);
OPTIMIZE TABLE test FINAL;
SELECT part_name FROM system.parts where table='test' and active and database = currentDatabase();
ALTER TABLE test DETACH PART 'all_1_1_1';
ALTER TABLE test ATTACH PART 'all_1_1_1';
SELECT part_name FROM system.parts where table='test' and active and database = currentDatabase();

-- Same as above, but with attach partition (different code path, should be tested as well)
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test VALUES (1), (2), (3);
OPTIMIZE TABLE test FINAL;
SELECT part_name FROM system.parts where table='test' and active and database = currentDatabase();
ALTER TABLE test DETACH PART 'all_1_1_1';
ALTER TABLE test ATTACH PARTITION tuple();
SELECT part_name FROM system.parts where table='test' and active and database = currentDatabase();
