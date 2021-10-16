DROP TABLE IF EXISTS part_header_r1;
DROP TABLE IF EXISTS part_header_r2;

SET replication_alter_partitions_sync = 2;

CREATE TABLE part_header_r1(x UInt32, y UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/'||currentDatabase()||'/test_00814/part_header/{shard}', '1{replica}') ORDER BY x
    SETTINGS use_minimalistic_part_header_in_zookeeper = 0,
             old_parts_lifetime = 1,
             cleanup_delay_period = 0,
             cleanup_delay_period_random_add = 0;
CREATE TABLE part_header_r2(x UInt32, y UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/'||currentDatabase()||'/test_00814/part_header/{shard}', '2{replica}') ORDER BY x
    SETTINGS use_minimalistic_part_header_in_zookeeper = 1,
             old_parts_lifetime = 1,
             cleanup_delay_period = 0,
             cleanup_delay_period_random_add = 0;

SELECT '*** Test fetches ***';
INSERT INTO part_header_r1 VALUES (1, 1);
INSERT INTO part_header_r2 VALUES (2, 2);
SYSTEM SYNC REPLICA part_header_r1;
SYSTEM SYNC REPLICA part_header_r2;
SELECT '*** replica 1 ***';
SELECT x, y FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, y FROM part_header_r2 ORDER BY x;

SELECT '*** Test merges ***';
OPTIMIZE TABLE part_header_r1;
SYSTEM SYNC REPLICA part_header_r2;
SELECT '*** replica 1 ***';
SELECT _part, x FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT _part, x FROM part_header_r2 ORDER BY x;

SELECT sleep(3) FORMAT Null;

SELECT '*** Test part removal ***';
SELECT '*** replica 1 ***';
SELECT name FROM system.parts WHERE active AND database = currentDatabase() AND table = 'part_header_r1';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/test_00814/part_header/s1/replicas/1r1/parts';
SELECT '*** replica 2 ***';
SELECT name FROM system.parts WHERE active AND database = currentDatabase() AND table = 'part_header_r2';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/'||currentDatabase()||'/test_00814/part_header/s1/replicas/1r1/parts';

SELECT '*** Test ALTER ***';
ALTER TABLE part_header_r1 MODIFY COLUMN y String;
SELECT '*** replica 1 ***';
SELECT x, length(y) FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, length(y) FROM part_header_r2 ORDER BY x;

SELECT '*** Test CLEAR COLUMN ***';
SET replication_alter_partitions_sync = 2;
ALTER TABLE part_header_r1 CLEAR COLUMN y IN PARTITION tuple();
SELECT '*** replica 1 ***';
SELECT x, length(y) FROM part_header_r1 ORDER BY x;
SELECT '*** replica 2 ***';
SELECT x, length(y) FROM part_header_r2 ORDER BY x;

DROP TABLE part_header_r1;
DROP TABLE part_header_r2;
