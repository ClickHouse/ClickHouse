-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- EXPLAIN output may differ

-- { echo }

DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree PARTITION BY x ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);

-- With partition pruning enabled (default): MinMax and Partition should prune
EXPLAIN indexes = 1 SELECT * FROM t WHERE x = 1
SETTINGS use_partition_pruning = 1;

-- With partition pruning disabled: both MinMax and Partition should show always true
EXPLAIN indexes = 1 SELECT * FROM t WHERE x = 1
SETTINGS use_partition_pruning = 0;

-- Verify correctness: results must be the same regardless of the setting
SELECT * FROM t WHERE x = 1 SETTINGS use_partition_pruning = 1;
SELECT * FROM t WHERE x = 1 SETTINGS use_partition_pruning = 0;

-- This is difficult to test that partition's _minmax_count_projection optimization is disabled
-- so we do the correctness check
-- pruning disabled should still return the correct count
SELECT count() FROM t WHERE x = 1 SETTINGS optimize_use_implicit_projections = 1, use_partition_pruning = 1;
SELECT count() FROM t WHERE x = 1 SETTINGS optimize_use_implicit_projections = 1, use_partition_pruning = 0;
