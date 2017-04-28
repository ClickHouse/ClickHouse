DROP TABLE IF EXISTS test.r1;
DROP TABLE IF EXISTS test.r2;
DROP TABLE IF EXISTS test.r3;

CREATE TABLE test.r1 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r1', d, x, 111);

INSERT INTO test.r1 VALUES ('2014-10-02', 1);
INSERT INTO test.r1 VALUES ('2014-10-01', 2);

SELECT '--- two rows below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';  -- 2
SELECT d, x FROM test.r1 ORDER BY d, x;

ALTER TABLE test.r1 DETACH PARTITION 201410;

SELECT '--- zero rows below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';  -- empty result
SELECT d, x FROM test.r1 ORDER BY d, x;	                                    -- empty result

ALTER TABLE test.r1 ATTACH PART '20141001_20141001_201_201_0';
SELECT '--- one row below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';  -- 1
SELECT d, x FROM test.r1 ORDER BY d, x;

ALTER TABLE test.r1 ATTACH PART '20141002_20141002_200_200_0';
SELECT '--- two rows below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';  -- 2
SELECT d, x FROM test.r1 ORDER BY d, x;

SET replication_alter_partitions_sync = 2;

SELECT '--- two rows below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1' AND active;   -- 2
SELECT d, x FROM test.r1 ORDER BY d, x;

CREATE TABLE test.r2 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r2', d, x, 111);
CREATE TABLE test.r3 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r3', d, x, 111);

INSERT INTO test.r2 VALUES ('2015-01-02', 3);
INSERT INTO test.r2 VALUES ('2015-01-01', 4);


SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2'; -- 4
SELECT d, x FROM test.r2 ORDER BY d, x;

ALTER TABLE test.r2 DETACH PARTITION 201501;

SELECT '--- 2 * 3 rows below --';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2'; -- 2

SELECT d, x FROM test.r1 ORDER BY d, x;
SELECT d, x FROM test.r2 ORDER BY d, x;
SELECT d, x FROM test.r3 ORDER BY d, x;

ALTER TABLE test.r1 ATTACH PARTITION 201501;

SELECT '--- 4 * 3 rows below --';
SELECT d, x FROM test.r1 ORDER BY d, x;
SELECT d, x FROM test.r2 ORDER BY d, x;
SELECT d, x FROM test.r3 ORDER BY d, x;

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1' AND active; -- 4
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2' AND active; -- 4
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r3' AND active; -- 4

SELECT sum(rows) FROM system.parts WHERE database = 'test' AND table = 'r1' AND active; -- 4

DROP TABLE test.r1;
DROP TABLE test.r2;
DROP TABLE test.r3;
