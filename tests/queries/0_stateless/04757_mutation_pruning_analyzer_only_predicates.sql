-- Tags: zookeeper, no-old-analyzer
-- Mutation predicates that only the query-tree analyzer can resolve (e.g. qualified column
-- names) must be accepted by the partition pruning analysis too, since the analysis runs
-- before the mutation and follows the same analyzer selection as the mutation execution.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;

DROP TABLE IF EXISTS t_mut_prune_analyzer;

CREATE TABLE t_mut_prune_analyzer (p UInt8, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_analyzer', 'r1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_mut_prune_analyzer VALUES (1, 0);
INSERT INTO t_mut_prune_analyzer VALUES (2, 0);

SELECT 'qualified partition key column in predicate';
ALTER TABLE t_mut_prune_analyzer UPDATE y = 1 WHERE t_mut_prune_analyzer.p = 1;
SELECT * FROM t_mut_prune_analyzer ORDER BY p;

-- The qualified name must not defeat the pruning itself either.
SELECT 'affected partitions';
SELECT arraySort(block_numbers.partition_id) AS partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_analyzer' AND NOT is_killed
ORDER BY mutation_id;

SELECT 'EXISTS subquery in predicate';
SET allow_nondeterministic_mutations = 1;
ALTER TABLE t_mut_prune_analyzer UPDATE y = y + 10 WHERE exists((SELECT * FROM system.one));
SELECT * FROM t_mut_prune_analyzer ORDER BY p;

DROP TABLE t_mut_prune_analyzer SYNC;
