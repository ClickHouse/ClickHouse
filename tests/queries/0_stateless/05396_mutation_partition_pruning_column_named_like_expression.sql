-- Tags: zookeeper
-- Mutation partition pruning does not take a column named like an expression for that expression.

SET optimize_mutations_with_partition_pruning = 1;

DROP TABLE IF EXISTS t_rpart;

CREATE TABLE t_rpart (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rpart', 'r1') PARTITION BY `equals(a, b)` ORDER BY k;
INSERT INTO t_rpart VALUES (1, 1, 10, 0), (1, 2, 20, 1), (3, 3, 30, 1), (4, 5, 40, 0);
ALTER TABLE t_rpart DELETE WHERE `equals(a, b)` = 0 AND a = b SETTINGS mutations_sync = 2;
SELECT k FROM t_rpart ORDER BY k;
SELECT arraySort(block_numbers.partition_id) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_rpart' AND NOT is_killed;

DROP TABLE t_rpart SYNC;
