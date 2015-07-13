DROP TABLE IF EXISTS test.parallel_replicas;

CREATE TABLE test.parallel_replicas (d Date DEFAULT today(), x UInt32, u UInt64, s String) ENGINE = MergeTree(d, cityHash64(u, s), (x, d, cityHash64(u, s)), 8192);
INSERT INTO test.parallel_replicas (x, u, s) SELECT toUInt32(number / 1000) AS x, toUInt64(number % 1000) AS u, toString(rand64()) AS s FROM system.numbers LIMIT 1000000;

SELECT count() FROM test.parallel_replicas;
SELECT count() > 0 FROM test.parallel_replicas SAMPLE 0.5;

SET parallel_replicas_count = 2;
SELECT count() > 0 FROM test.parallel_replicas;

SET parallel_replica_offset = 1;
SELECT count() FROM test.parallel_replicas;
