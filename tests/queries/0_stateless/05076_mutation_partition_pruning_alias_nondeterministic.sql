-- Tags: zookeeper
-- https://github.com/ClickHouse/ClickHouse/issues/117115
-- Mutation partition pruning refuses a predicate containing a query-time non-deterministic function,
-- because the pruning pass folds it at submission time while the asynchronous execution evaluates it
-- again, later. The check walked the raw predicate only, so `now` hidden inside an `ALIAS` column's
-- definition was invisible - even though the pruning analysis deliberately resolves that definition
-- against the storage.

SET mutations_sync = 0, allow_nondeterministic_mutations = 1;

DROP TABLE IF EXISTS t_prune_alias;
CREATE TABLE t_prune_alias (p UInt32, x UInt64, r UInt32 ALIAS toUnixTimestamp(now()))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_alias', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_prune_alias (p, x) VALUES (1, 1), (2, 2);

SYSTEM STOP REPLICATION QUEUES t_prune_alias;

-- Must cover every partition: `now` is evaluated again when the mutation runs.
ALTER TABLE t_prune_alias DELETE WHERE p < r;
-- A deterministic predicate is still pruned.
ALTER TABLE t_prune_alias DELETE WHERE p = 1;

SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias' ORDER BY mutation_id;

SELECT 'deterministic alias';
DROP TABLE IF EXISTS t_prune_alias_ok;
CREATE TABLE t_prune_alias_ok (p UInt32, x UInt64, q UInt32 ALIAS p + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_alias_ok', 'r1')
PARTITION BY p ORDER BY x;
INSERT INTO t_prune_alias_ok (p, x) VALUES (1, 1), (2, 2);
SYSTEM STOP REPLICATION QUEUES t_prune_alias_ok;
ALTER TABLE t_prune_alias_ok DELETE WHERE q = 2;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias_ok' ORDER BY mutation_id;

DROP TABLE t_prune_alias;
DROP TABLE t_prune_alias_ok;
