-- Tags: zookeeper, no-old-analyzer
-- The partition pruning analysis must follow the analyzer selection of the *background* mutation
-- execution (which builds its context from the background context and does not see session-only
-- settings), not of the submitting session. Here the session turns the analyzer off, yet the
-- analyzer-only predicate (a qualified column name) must still be accepted and pruned, because
-- the mutation itself will run under the server-default analyzer mode.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;
SET allow_experimental_analyzer = 0;

DROP TABLE IF EXISTS t_mut_prune_bg_analyzer;

CREATE TABLE t_mut_prune_bg_analyzer (p UInt8, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_bg_analyzer', 'r1')
PARTITION BY p ORDER BY tuple();

INSERT INTO t_mut_prune_bg_analyzer VALUES (1, 0);
INSERT INTO t_mut_prune_bg_analyzer VALUES (2, 0);

SELECT 'qualified column predicate with session analyzer off';
ALTER TABLE t_mut_prune_bg_analyzer UPDATE y = 1 WHERE t_mut_prune_bg_analyzer.p = 1;
SELECT * FROM t_mut_prune_bg_analyzer ORDER BY p;

SELECT 'affected partitions';
SELECT arraySort(block_numbers.partition_id) AS partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_bg_analyzer' AND NOT is_killed
ORDER BY mutation_id;

DROP TABLE t_mut_prune_bg_analyzer SYNC;
