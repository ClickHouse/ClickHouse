DROP TABLE IF EXISTS test.replicated_optimize1;
DROP TABLE IF EXISTS test.replicated_optimize2;
CREATE TABLE test.replicated_optimize1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/optimize', 'r1', d, k, 8192);
CREATE TABLE test.replicated_optimize2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/optimize', 'r2', d, k, 8192);

OPTIMIZE TABLE test.replicated_optimize1 FINAL;

DROP TABLE test.replicated_optimize1;
DROP TABLE test.replicated_optimize2;
