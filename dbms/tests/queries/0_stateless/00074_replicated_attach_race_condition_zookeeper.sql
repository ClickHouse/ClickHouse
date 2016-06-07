SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS test.attach_r1;
DROP TABLE IF EXISTS test.attach_r2;

CREATE TABLE test.attach_r1 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/attach', 'r1', d, d, 8192);
INSERT INTO test.attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');

SELECT d FROM test.attach_r1 ORDER BY d;

ALTER TABLE test.attach_r1 DETACH PARTITION 201402;

SELECT '---';
SELECT d FROM test.attach_r1 ORDER BY d;

CREATE TABLE test.attach_r2 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/01/attach', 'r2', d, d, 8192);

ALTER TABLE test.attach_r1 ATTACH PARTITION 201402;

SELECT '---';
SELECT d FROM test.attach_r1 ORDER BY d;

-- так как куски скачиваются параллельно, в произвольном порядке, то наличие кусков отличных от ATTACH-енного в данный момент не гарантируется
-- наличие ATTACH-енного куска гарантируется настройкой replication_alter_partitions_sync - проверим это
SELECT d FROM test.attach_r2 WHERE d = '2014-02-01' ORDER BY d;

ALTER TABLE test.attach_r1 DETACH PARTITION 201401;
ALTER TABLE test.attach_r1 DETACH PARTITION 201403;

SELECT '---';
SELECT d FROM test.attach_r1 ORDER BY d;
SELECT d FROM test.attach_r2 ORDER BY d;

ALTER TABLE test.attach_r1 ATTACH PARTITION 201401;
ALTER TABLE test.attach_r1 ATTACH PARTITION 201403;

SELECT '---';
SELECT d FROM test.attach_r1 ORDER BY d;
SELECT d FROM test.attach_r2 ORDER BY d;

DROP TABLE test.attach_r1;
DROP TABLE test.attach_r2;
