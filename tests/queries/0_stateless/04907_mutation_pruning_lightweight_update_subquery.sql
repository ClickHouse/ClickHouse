-- Tags: zookeeper
-- A lightweight update must not prune a predicate that contains a deferred subquery: the
-- independently evaluated subquery can change after partition block numbers are allocated.

SET enable_lightweight_update = 1;
SET optimize_mutations_with_partition_pruning = 1;
SET allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS t_mut_prune_lwu_subquery SYNC;
DROP TABLE IF EXISTS t_mut_prune_lwu_source SYNC;

CREATE TABLE t_mut_prune_lwu_subquery (p UInt8, x UInt8, y UInt8)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_lwu_subquery', 'r1')
PARTITION BY p ORDER BY x
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE t_mut_prune_lwu_source (p UInt8)
ENGINE = MergeTree ORDER BY p;

INSERT INTO t_mut_prune_lwu_subquery VALUES (1, 1, 0), (2, 2, 0), (3, 3, 0);
INSERT INTO t_mut_prune_lwu_source VALUES (1), (3);

UPDATE t_mut_prune_lwu_subquery SET y = 1 WHERE p IN (SELECT p FROM t_mut_prune_lwu_source);
SELECT * FROM t_mut_prune_lwu_subquery ORDER BY p;

DROP TABLE t_mut_prune_lwu_source SYNC;
DROP TABLE t_mut_prune_lwu_subquery SYNC;
