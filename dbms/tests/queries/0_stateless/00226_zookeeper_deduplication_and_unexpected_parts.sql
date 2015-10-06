DROP TABLE IF EXISTS test.deduplication;
CREATE TABLE test.deduplication (d Date DEFAULT '2015-01-01', x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/deduplication', 'r1', d, x, 1);

INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);
INSERT INTO test.deduplication (x) VALUES (1);

SELECT * FROM test.deduplication;

DETACH TABLE test.deduplication;
ATTACH TABLE test.deduplication (d Date DEFAULT '2015-01-01', x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/deduplication', 'r1', d, x, 1);

SELECT * FROM test.deduplication;

DROP TABLE test.deduplication;
