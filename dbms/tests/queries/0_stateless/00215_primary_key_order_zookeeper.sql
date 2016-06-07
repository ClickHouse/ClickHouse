DROP TABLE IF EXISTS test.primary_key;
CREATE TABLE test.primary_key (d Date DEFAULT today(), x Int8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/primary_key', 'r1', d, -x, 1);

INSERT INTO test.primary_key (x) VALUES (1), (2), (3);
INSERT INTO test.primary_key (x) VALUES (1), (3), (2);
INSERT INTO test.primary_key (x) VALUES (2), (1), (3);
INSERT INTO test.primary_key (x) VALUES (2), (3), (1);
INSERT INTO test.primary_key (x) VALUES (3), (1), (2);
INSERT INTO test.primary_key (x) VALUES (3), (2), (1);

SELECT x FROM test.primary_key ORDER BY x;
SELECT x FROM test.primary_key WHERE -x < -1 ORDER BY x;

DROP TABLE test.primary_key;
