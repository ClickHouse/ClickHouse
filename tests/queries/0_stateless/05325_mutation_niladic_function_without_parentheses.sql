-- Tags: zookeeper, no-shared-merge-tree, no-replicated-database
-- no-shared-merge-tree: the check of non-deterministic mutations applies to Replicated* engines only
-- no-replicated-database: a Replicated database rejects an ALTER that mixes column and mutation commands
-- https://github.com/ClickHouse/ClickHouse/issues/124002
-- A function written without parentheses (`today`, `CURRENT_DATE`, `now`) is as non-deterministic in a mutation
-- as its call with parentheses.

SET mutations_sync = 0;
SET optimize_mutations_with_partition_pruning = 1;

SELECT 'not pruned';
SET allow_nondeterministic_mutations = 1, mutations_execute_nondeterministic_on_initiator = 0;
CREATE TABLE t_prune (d Date, k UInt64, r Date ALIAS today)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_prune', 'r1') PARTITION BY d ORDER BY k;
INSERT INTO t_prune (d, k) VALUES ('2000-01-01', 1), ('2100-01-01', 2);
SYSTEM STOP REPLICATION QUEUES t_prune;
ALTER TABLE t_prune DELETE WHERE d < today();
ALTER TABLE t_prune DELETE WHERE d < today;
ALTER TABLE t_prune DELETE WHERE d < CURRENT_DATE;
ALTER TABLE t_prune DELETE WHERE toDateTime(d) < now;
ALTER TABLE t_prune DELETE WHERE d < r;
ALTER TABLE t_prune DELETE WHERE d < '2050-01-01' AND arrayExists(x -> x <= today, [d]);
-- A deterministic predicate is still pruned.
ALTER TABLE t_prune DELETE WHERE d = '2000-01-01';
SYSTEM SYNC REPLICA t_prune PULL;
SELECT mutation_id, command, arraySort(block_numbers.partition_id) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_prune' ORDER BY mutation_id;

SELECT 'rejected on a replicated table';
SET allow_nondeterministic_mutations = 0;
CREATE TABLE t_guard (d Date, k UInt64, v UInt64 DEFAULT 0, r Date ALIAS today)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_guard', 'r1') PARTITION BY d ORDER BY k;
INSERT INTO t_guard (d, k) VALUES ('2000-01-01', 1), ('2100-01-01', 2);
SYSTEM STOP REPLICATION QUEUES t_guard;
ALTER TABLE t_guard DELETE WHERE d < today(); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE d < today; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE d < CURRENT_DATE; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE toDateTime(d) < now; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE toString(k) = user; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE arrayExists(x -> x < today, [d]); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE d IN toDate(today); -- { serverError BAD_ARGUMENTS }
-- An alias inside a lambda does not bind the name outside of it.
ALTER TABLE t_guard DELETE WHERE arrayExists(x -> (x AS today) > '2000-01-01', [d]) AND d < today; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard DELETE WHERE d < (today AS today); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard UPDATE v = toUnixTimestamp(CURRENT_TIMESTAMP) WHERE 1; -- { serverError BAD_ARGUMENTS }
DELETE FROM t_guard WHERE d < today SETTINGS lightweight_deletes_sync = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_nested (d Date, k UInt64, today Nested(a UInt8), `now.x` UInt8, k1 UInt64 ALIAS k + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_nested', 'r1') ORDER BY k;
SYSTEM STOP REPLICATION QUEUES t_nested;
-- A Nested column binds its name, a column whose name only starts with it does not.
ALTER TABLE t_nested DELETE WHERE toDateTime(d) < now; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_nested DELETE WHERE notEmpty(today);
-- The definition of an `ALIAS` column is checked too.
ALTER TABLE t_guard DELETE WHERE d < r; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_guard UPDATE v = toUInt64(r) WHERE 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_nested DELETE WHERE k1 = 0;

SELECT 'replaced with the value on the initiator when written with parentheses';
SET mutations_execute_nondeterministic_on_initiator = 1;
ALTER TABLE t_guard DELETE WHERE d < CURRENT_DATE() AND k = 100;
ALTER TABLE t_guard DELETE WHERE d < CURRENT_DATE AND k = 101; -- { serverError BAD_ARGUMENTS }
SYSTEM SYNC REPLICA t_guard PULL;
SELECT replaceRegexpAll(command, '\\d{5,}', 'N') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_guard' ORDER BY mutation_id;

SELECT 'a column, a lambda parameter or an alias with the same name';
CREATE TABLE t_bound (p UInt8, user String, today Date, v UInt64 DEFAULT 0)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_bound', 'r1') PARTITION BY p ORDER BY tuple();
INSERT INTO t_bound (p, user, today) VALUES (1, 'a', '2000-01-01'), (2, 'b', '2000-01-01');
SYSTEM STOP REPLICATION QUEUES t_bound;
-- Still `allow_nondeterministic_mutations = 0`, and the values are replaced on the initiator.
ALTER TABLE t_bound UPDATE v = 1 WHERE p = 1 AND user = 'a' AND today = '2000-01-01';
ALTER TABLE t_bound UPDATE v = 2 WHERE p = 1 AND arrayExists(now -> now > 0, [v]);
ALTER TABLE t_bound UPDATE v = 3 WHERE (p AS now) = 1 AND now = 1;
ALTER TABLE t_bound UPDATE v = (p AS now) WHERE now = 1;
SYSTEM SYNC REPLICA t_bound PULL;
SELECT mutation_id, command, arraySort(block_numbers.partition_id) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_bound' ORDER BY mutation_id;

SELECT 'a column added, renamed or dropped by the same query';
SET mutations_sync = 2, enable_shared_storage_snapshot_in_query = 1;
CREATE TABLE t_alter (d Date, k UInt64, v1 UInt64 DEFAULT 0, v2 UInt64 DEFAULT 0, x UInt64 DEFAULT 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_alter', 'r1') ORDER BY k;
INSERT INTO t_alter (d, k) VALUES ('2000-01-01', 1);
ALTER TABLE t_alter ADD COLUMN today Date DEFAULT '2000-01-01', UPDATE v1 = 7 WHERE today = '2000-01-01';
ALTER TABLE t_alter RENAME COLUMN x TO now, UPDATE v2 = 8 WHERE now = 1;
ALTER TABLE t_alter DROP COLUMN today, UPDATE v1 = 9 WHERE d < today; -- { serverError BAD_ARGUMENTS }
SELECT k, v1, v2 FROM t_alter;

DROP TABLE t_prune SYNC;
DROP TABLE t_guard SYNC;
DROP TABLE t_bound SYNC;
DROP TABLE t_alter SYNC;
DROP TABLE t_nested SYNC;
