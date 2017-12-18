SET replication_alter_partitions_sync = 2;

DROP TABLE IF EXISTS test.attach_r1;
DROP TABLE IF EXISTS test.attach_r2;

CREATE TABLE test.attach_r1 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01/attach', 'r1', d, d, 8192);
CREATE TABLE test.attach_r2 (d Date) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/01/attach', 'r2', d, d, 8192);

INSERT INTO test.attach_r1 VALUES ('2014-01-01'), ('2014-02-01'), ('2014-03-01');

SELECT d FROM test.attach_r1 ORDER BY d;

ALTER TABLE test.attach_r2 DROP PARTITION 201402;

SELECT d FROM test.attach_r1 ORDER BY d;

DROP TABLE test.attach_r1;
DROP TABLE test.attach_r2;
