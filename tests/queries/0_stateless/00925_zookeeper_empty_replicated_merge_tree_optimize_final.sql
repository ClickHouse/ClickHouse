DROP TABLE IF EXISTS replicated_optimize1;
DROP TABLE IF EXISTS replicated_optimize2;
CREATE TABLE replicated_optimize1 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/optimize', 'r1', d, k, 8192);
CREATE TABLE replicated_optimize2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test/optimize', 'r2', d, k, 8192);

OPTIMIZE TABLE replicated_optimize1 FINAL;

DROP TABLE replicated_optimize1;
DROP TABLE replicated_optimize2;
