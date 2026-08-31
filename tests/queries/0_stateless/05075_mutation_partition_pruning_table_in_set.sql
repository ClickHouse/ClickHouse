-- Tags: zookeeper
-- https://github.com/ClickHouse/ClickHouse/issues/117113
-- Mutation partition pruning must leave a predicate with a deferred `IN` set unpruned: the pruning
-- pass and the asynchronous mutation execution evaluate the set independently, so rows in a partition
-- that matches only the execution-time set would have no block number and escape the mutation. A
-- parsed `IN some_table` carries a plain `ASTIdentifier`, not an `ASTTableIdentifier`, so the guard
-- used to miss exactly the form the mutation validation lets through - an explicit subquery is
-- rejected up front, a bare table identifier is not.

SET mutations_sync = 0;

DROP TABLE IF EXISTS t_prune_in_table_keys;
DROP TABLE IF EXISTS t_prune_in_table;
CREATE TABLE t_prune_in_table_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_prune_in_table (p UInt8, x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_in_table', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_prune_in_table_keys VALUES (1);
INSERT INTO t_prune_in_table VALUES (1, 1), (2, 2);

SYSTEM STOP REPLICATION QUEUES t_prune_in_table;

-- The set holds only partition 1 right now, but the mutation has to cover every partition: it is
-- re-evaluated when the mutation runs.
ALTER TABLE t_prune_in_table DELETE WHERE p IN t_prune_in_table_keys;
-- A literal enumeration is a stable constant set, so it is still pruned to partition 1.
ALTER TABLE t_prune_in_table DELETE WHERE p IN (1);

SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_in_table' ORDER BY mutation_id;

DROP TABLE t_prune_in_table;
DROP TABLE t_prune_in_table_keys;
