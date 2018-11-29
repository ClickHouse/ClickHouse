DROP TABLE IF EXISTS test.replicated_truncate1;
DROP TABLE IF EXISTS test.replicated_truncate2;

CREATE TABLE test.replicated_truncate1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/truncate', 'r1', d, k, 8192);
CREATE TABLE test.replicated_truncate2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/truncate', 'r2', d, k, 8192);

SELECT '======Before Truncate======';
INSERT INTO test.replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA test.replicated_truncate2;

SELECT * FROM test.replicated_truncate1 ORDER BY k;
SELECT * FROM test.replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Empty======';
TRUNCATE TABLE test.replicated_truncate1;

SELECT * FROM test.replicated_truncate1 ORDER BY k;
SELECT * FROM test.replicated_truncate2 ORDER BY k;

SELECT '======After Truncate And Insert Data======';
INSERT INTO test.replicated_truncate1 VALUES ('2015-01-01', 10, 42);

SYSTEM SYNC REPLICA test.replicated_truncate2;

SELECT * FROM test.replicated_truncate1 ORDER BY k;
SELECT * FROM test.replicated_truncate2 ORDER BY k;

DROP TABLE IF EXISTS test.replicated_truncate1;
DROP TABLE IF EXISTS test.replicated_truncate2;
