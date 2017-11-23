DROP TABLE IF EXISTS test.deduplication_by_partition;
CREATE TABLE test.deduplication_by_partition(d Date, x UInt32) ENGINE =
    ReplicatedMergeTree('/clickhouse/tables/test/deduplication_by_partition', 'r1', d, x, 8192);

INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO test.deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO test.deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);

SELECT '*** Before DROP PARTITION ***';

SELECT * FROM test.deduplication_by_partition ORDER BY d, x;

ALTER TABLE test.deduplication_by_partition DROP PARTITION 200001;

SELECT '*** After DROP PARTITION ***';

SELECT * FROM test.deduplication_by_partition ORDER BY d, x;

INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 1);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 2), ('2000-01-01', 3);
INSERT INTO test.deduplication_by_partition VALUES ('2000-01-01', 4);
INSERT INTO test.deduplication_by_partition VALUES ('2000-02-01', 3), ('2000-02-01', 4), ('2000-02-01', 5);
INSERT INTO test.deduplication_by_partition VALUES ('2000-02-01', 6), ('2000-02-01', 7);

SELECT '*** After INSERT ***';

SELECT * FROM test.deduplication_by_partition ORDER BY d, x;

DROP TABLE test.deduplication_by_partition;
