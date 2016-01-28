DROP TABLE IF EXISTS test.r1;
DROP TABLE IF EXISTS test.r2;
DROP TABLE IF EXISTS test.r3;

CREATE TABLE test.r1 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r1', d, x, 111);

INSERT INTO test.r1 VALUES ('2014-10-02', 1);
INSERT INTO test.r1 VALUES ('2014-10-01', 2);

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';
SELECT d, x FROM test.r1 ORDER BY d, x;

ALTER TABLE test.r1 DETACH PARTITION 201410;

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';
SELECT d, x FROM test.r1 ORDER BY d, x;

ALTER TABLE test.r1 ATTACH PART '20141001_20141001_201_201_0';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';
SELECT d, x FROM test.r1 ORDER BY d, x;

ALTER TABLE test.r1 ATTACH PART '20141002_20141002_200_200_0';
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1';
SELECT d, x FROM test.r1 ORDER BY d, x;


SET replication_alter_partitions_sync = 1;

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1' AND active;
SELECT d, x FROM test.r1 ORDER BY d, x;

CREATE TABLE test.r2 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r2', d, x, 111);
CREATE TABLE test.r3 (d Date DEFAULT today(), x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/r/', 'r3', d, x, 111);

INSERT INTO test.r2 VALUES ('2015-01-02', 3);
INSERT INTO test.r2 VALUES ('2015-01-01', 4);

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2';
SELECT d, x FROM test.r2 ORDER BY d, x;

ALTER TABLE test.r2 DETACH PARTITION 201501;

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2';

SELECT d, x FROM test.r1 ORDER BY d, x;
SELECT d, x FROM test.r2 ORDER BY d, x;
SELECT d, x FROM test.r3 ORDER BY d, x;

ALTER TABLE test.r1 ATTACH PARTITION 201501;

SELECT d, x FROM test.r1 ORDER BY d, x;
SELECT d, x FROM test.r2 ORDER BY d, x;
SELECT d, x FROM test.r3 ORDER BY d, x;

SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r1' AND active;
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r2' AND active;
SELECT count() FROM system.parts WHERE database = 'test' AND table = 'r3' AND active;

DROP TABLE test.r1;
DROP TABLE test.r2;
DROP TABLE test.r3;
