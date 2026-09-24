-- Tags: zookeeper
-- https://github.com/ClickHouse/ClickHouse/issues/117115
-- Mutation partition pruning refuses a predicate containing a query-time non-deterministic function,
-- because the pruning pass folds it at submission time while the asynchronous execution evaluates it
-- again, later. The check walked the raw predicate only, so `now` hidden inside an `ALIAS` column's
-- definition was invisible - even though the pruning analysis deliberately resolves that definition
-- against the storage.

SET mutations_sync = 0, allow_nondeterministic_mutations = 1;
-- The pruning this file is about is the feature under test, so it cannot be left to the randomizer:
-- with it off no partition is ever pruned and every expectation below gains the second partition.
SET optimize_mutations_with_partition_pruning = 1;

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

-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_alias PULL;
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
-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_alias_ok PULL;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias_ok' ORDER BY mutation_id;

SELECT 'qualified alias';
-- The predicate is raw text, so the column can be spelled with a table qualifier. The analyzer
-- strips it and resolves the very same `ALIAS` definition, so the check has to see it too.
DROP TABLE IF EXISTS t_prune_alias_qualified;
CREATE TABLE t_prune_alias_qualified (p UInt32, x UInt64, r UInt32 ALIAS toUnixTimestamp(now()))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_alias_qualified', 'r1')
PARTITION BY p ORDER BY x;
INSERT INTO t_prune_alias_qualified (p, x) VALUES (1, 1), (2, 2);
SYSTEM STOP REPLICATION QUEUES t_prune_alias_qualified;
ALTER TABLE t_prune_alias_qualified DELETE WHERE p < t_prune_alias_qualified.r;
-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_alias_qualified PULL;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias_qualified' ORDER BY mutation_id;

SELECT 'alias subcolumn';
-- Addressing a subcolumn of an `ALIAS` column is another spelling of the same definition.
DROP TABLE IF EXISTS t_prune_alias_subcolumn;
CREATE TABLE t_prune_alias_subcolumn (p UInt32, x UInt64,
    r Tuple(a UInt32, b UInt32) ALIAS tuple(toUnixTimestamp(now()), toUInt32(0)))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_alias_subcolumn', 'r1')
PARTITION BY p ORDER BY x;
INSERT INTO t_prune_alias_subcolumn (p, x) VALUES (1, 1), (2, 2);
SYSTEM STOP REPLICATION QUEUES t_prune_alias_subcolumn;
ALTER TABLE t_prune_alias_subcolumn DELETE WHERE p < r.a;
-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_alias_subcolumn PULL;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias_subcolumn' ORDER BY mutation_id;

SELECT 'stored default';
-- A stored `DEFAULT` (or `MATERIALIZED`) column is read as it was written: its definition is not
-- re-expanded when the predicate is analyzed, so a non-deterministic function inside it cannot make
-- the analysis and the execution disagree, and a literal predicate over it must still prune.
DROP TABLE IF EXISTS t_prune_default;
CREATE TABLE t_prune_default (d Date DEFAULT today(), x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_default', 'r1')
PARTITION BY toYYYYMM(d) ORDER BY x;
INSERT INTO t_prune_default (d, x) VALUES ('2026-01-15', 1), ('2026-02-15', 2);
SYSTEM STOP REPLICATION QUEUES t_prune_default;
ALTER TABLE t_prune_default DELETE WHERE d = '2026-01-15';
-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_default PULL;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_default' ORDER BY mutation_id;

SELECT 'lambda parameter shadows alias';
-- A lambda keeps its parameters as plain identifiers in the predicate. Inside the body such a name
-- is the parameter, not the storage column of the same name, so an unrelated non-deterministic
-- `ALIAS` column `x` must not disable pruning for `arrayExists(x -> x = 1, arr)`; a reference to
-- the alias through another name inside the same lambda must still be seen.
DROP TABLE IF EXISTS t_prune_alias_lambda;
CREATE TABLE t_prune_alias_lambda (p UInt32, k UInt64, arr Array(UInt32), x UInt32 ALIAS toUnixTimestamp(now()))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune_alias_lambda', 'r1')
PARTITION BY p ORDER BY k;
INSERT INTO t_prune_alias_lambda (p, k, arr) VALUES (1, 1, [1]), (2, 2, [2]);
SYSTEM STOP REPLICATION QUEUES t_prune_alias_lambda;
ALTER TABLE t_prune_alias_lambda DELETE WHERE p = 1 AND arrayExists(x -> x = 1, arr);
ALTER TABLE t_prune_alias_lambda DELETE WHERE p = 1 AND arrayExists((x, y) -> x = y, arr, arr);
-- The parameter is bound only inside its own lambda: the same name outside is the column again.
ALTER TABLE t_prune_alias_lambda DELETE WHERE p = 1 AND arrayExists(x -> x = 1, arr) AND x > 0;
-- The alias referenced from inside the lambda body under a different parameter name is the column.
ALTER TABLE t_prune_alias_lambda DELETE WHERE p = 1 AND arrayExists(y -> y = x, arr);
-- The mutation entry is written to ZooKeeper by the `ALTER`, but it becomes visible in
-- `system.mutations` only after the replica pulls it, so pull it explicitly instead of racing.
SYSTEM SYNC REPLICA t_prune_alias_lambda PULL;
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_alias_lambda' ORDER BY mutation_id;

DROP TABLE t_prune_alias;
DROP TABLE t_prune_alias_lambda;
DROP TABLE t_prune_alias_ok;
DROP TABLE t_prune_alias_qualified;
DROP TABLE t_prune_alias_subcolumn;
DROP TABLE t_prune_default;
