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
SELECT mutation_id, `block_numbers.partition_id` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune_default' ORDER BY mutation_id;

DROP TABLE t_prune_alias;
DROP TABLE t_prune_alias_ok;
DROP TABLE t_prune_alias_qualified;
DROP TABLE t_prune_alias_subcolumn;
DROP TABLE t_prune_default;
