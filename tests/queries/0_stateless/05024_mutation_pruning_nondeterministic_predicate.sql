-- Tags: zookeeper
-- A query-time non-deterministic constant (e.g. `now`) in a mutation predicate is folded to
-- its submission-time value by the pruning analysis, but the stored mutation re-evaluates it
-- during the asynchronous execution - possibly much later, or on another replica. When such a
-- constant survives into the stored mutation entry (`allow_nondeterministic_mutations = 1`
-- without `mutations_execute_nondeterministic_on_initiator`), pruning must fall back to all
-- partitions instead of narrowing `system.mutations.block_numbers` to the submission-time
-- partition. A predicate rewritten to literals on the initiator stays prunable.

SET mutations_sync = 2;
SET optimize_mutations_with_partition_pruning = 1;
SET allow_nondeterministic_mutations = 1;
SET mutations_execute_nondeterministic_on_initiator = 0;

DROP TABLE IF EXISTS t_mut_prune_nondet;

CREATE TABLE t_mut_prune_nondet (p UInt8, x UInt64, y UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mut_prune_nondet', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_mut_prune_nondet VALUES (1, 1, 0);
INSERT INTO t_mut_prune_nondet VALUES (2, 2, 0);

SELECT 'a predicate with a query-time constant is not pruned';
ALTER TABLE t_mut_prune_nondet UPDATE y = 1 WHERE p = 1 AND now() >= toDateTime(0);
SELECT * FROM t_mut_prune_nondet ORDER BY p;

SELECT 'a predicate rewritten to literals on the initiator is pruned';
ALTER TABLE t_mut_prune_nondet UPDATE y = 2 WHERE p = 1 AND now() >= toDateTime(0)
SETTINGS mutations_execute_nondeterministic_on_initiator = 1;
SELECT * FROM t_mut_prune_nondet ORDER BY p;

SELECT 'affected partitions per mutation';
-- `system.mutations` has one row per command, so collapse by mutation.
SELECT arraySort(block_numbers.partition_id) AS partitions
FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mut_prune_nondet' AND NOT is_killed
GROUP BY mutation_id, partitions
ORDER BY mutation_id;

DROP TABLE t_mut_prune_nondet SYNC;
