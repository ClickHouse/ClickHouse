-- Tags: zookeeper
-- A single ALTER TABLE may carry several mutation commands. The partition scopes of the
-- commands are combined: explicit IN PARTITION targets and pruned predicate scopes are
-- unioned, and one command that cannot be scoped widens the mutation to all partitions.
-- The scope is observable in `system.mutations.block_numbers.partition_id`.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;

DROP TABLE IF EXISTS t_mut_prune_multi;

CREATE TABLE t_mut_prune_multi (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_multi', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_mut_prune_multi VALUES (1, 1, 0);
INSERT INTO t_mut_prune_multi VALUES (2, 2, 0);
INSERT INTO t_mut_prune_multi VALUES (3, 3, 0);

SELECT 'two pruned commands in one statement';
ALTER TABLE t_mut_prune_multi UPDATE y = 1 WHERE p = 1, UPDATE y = 2 WHERE p = 2;
SELECT * FROM t_mut_prune_multi ORDER BY p;

SELECT 'mixed explicit and pruned command in one statement';
ALTER TABLE t_mut_prune_multi UPDATE y = y + 10 IN PARTITION 3 WHERE 1, UPDATE y = y + 20 WHERE p = 1;
SELECT * FROM t_mut_prune_multi ORDER BY p;

SELECT 'pruned command plus a command that cannot be scoped';
ALTER TABLE t_mut_prune_multi UPDATE y = y + 100 WHERE p = 2, UPDATE y = y + 200 WHERE x = 3;
SELECT * FROM t_mut_prune_multi ORDER BY p;

SELECT 'affected partitions per mutation';
-- `system.mutations` has one row per command, so a multi-command mutation must be collapsed.
SELECT arraySort(block_numbers.partition_id) AS partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_multi' AND NOT is_killed
GROUP BY mutation_id, partitions
ORDER BY mutation_id;

DROP TABLE t_mut_prune_multi SYNC;
