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
DROP TABLE IF EXISTS t_prune_in_table_alias;
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

-- A mutation entry becomes visible in `system.mutations` only after this replica pulls it from
-- ZooKeeper, which happens asynchronously; the pull is what `SYSTEM SYNC REPLICA ... PULL` waits
-- for, and it works while the replication queues are stopped.
SYSTEM SYNC REPLICA t_prune_in_table PULL;

SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_in_table' ORDER BY mutation_id;

-- The same deferred set can reach the pruning pass through a column default: the analysis expands
-- `ALIAS` columns against the storage, so the raw predicate mentions only the column name while the
-- set is still evaluated twice. A bare table identifier is not a subquery node, so such a column
-- definition passes the validation that rejects subqueries in column defaults.
CREATE TABLE t_prune_in_table_alias
(
    p UInt8,
    x UInt64,
    is_hit UInt8 ALIAS p IN t_prune_in_table_keys,
    is_one UInt8 ALIAS p IN (1)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_in_table_alias', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_prune_in_table_alias VALUES (1, 1), (2, 2);

SYSTEM STOP REPLICATION QUEUES t_prune_in_table_alias;

-- Not pruned: the `ALIAS` expression hides a deferred set.
ALTER TABLE t_prune_in_table_alias DELETE WHERE is_hit;
-- Pruned to partition 1: the `ALIAS` expression is a stable constant set.
ALTER TABLE t_prune_in_table_alias DELETE WHERE is_one;

SYSTEM SYNC REPLICA t_prune_in_table_alias PULL;

SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_in_table_alias' ORDER BY mutation_id;

DROP TABLE t_prune_in_table_alias;
DROP TABLE t_prune_in_table;
DROP TABLE t_prune_in_table_keys;
