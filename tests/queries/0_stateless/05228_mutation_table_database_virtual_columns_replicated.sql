-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: non-deterministic mutations are allowed with shared merge tree

-- `_table`, `_database` and `_disk_name` are virtual columns declared non-deterministic: replicas of one
-- `ReplicatedMergeTree` table may live under different local names or on different disks, so a mutation
-- that reads them would produce different parts on different replicas. Like non-deterministic functions,
-- they are refused on replicated tables unless `allow_nondeterministic_mutations` is set.
-- https://github.com/ClickHouse/ClickHouse/issues/102331
-- https://github.com/ClickHouse/ClickHouse/issues/119212

DROP TABLE IF EXISTS t_repl_05228 SYNC;

CREATE TABLE t_repl_05228 (key Int, value String, _sample_factor UInt8)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05228/t', 'r1') ORDER BY key;
INSERT INTO t_repl_05228 (key, value) VALUES (1, 'a'), (2, 'b'), (3, 'c');

ALTER TABLE t_repl_05228 UPDATE value = _table WHERE key = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 UPDATE value = _database WHERE key = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 UPDATE value = _disk_name WHERE key = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 DELETE WHERE _table = 't_repl_05228'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 DELETE WHERE _database != ''; -- { serverError BAD_ARGUMENTS }
DELETE FROM t_repl_05228 WHERE _table = 't_repl_05228'; -- { serverError BAD_ARGUMENTS }

-- A qualifier naming the mutated table does not hide the virtual column.
ALTER TABLE t_repl_05228 UPDATE value = t_repl_05228._table WHERE key = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 DELETE WHERE t_repl_05228._database != ''; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228 DELETE WHERE {CLICKHOUSE_DATABASE:Identifier}.t_repl_05228._table != ''; -- { serverError BAD_ARGUMENTS }

-- Inside a lambda body, the virtual column is still read unless a lambda parameter shadows it.
ALTER TABLE t_repl_05228 DELETE WHERE arrayExists(x -> x = _table, ['t_repl_05228']); -- { serverError BAD_ARGUMENTS }

-- Deterministic virtual columns are fine.
ALTER TABLE t_repl_05228 UPDATE value = _part WHERE key = 1 SETTINGS mutations_sync = 2;

-- A lambda parameter named like a non-deterministic virtual column does not read it.
ALTER TABLE t_repl_05228 DELETE WHERE arrayExists(_table -> _table = 0, [key]) SETTINGS mutations_sync = 2;

-- A real column shadowing a non-deterministic virtual is deterministic, so the replicated guard lets it
-- through. Reading it in a mutation then hits a separate pre-existing failure (see
-- `04510_mutation_query_plan_only_virtual_columns`), which is pinned here to show that it is not
-- `BAD_ARGUMENTS` from the guard.
ALTER TABLE t_repl_05228 DELETE WHERE _sample_factor = 7 SETTINGS validate_mutation_query = 1; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- The escape hatch applies to virtual columns the same way as to functions.
ALTER TABLE t_repl_05228 UPDATE value = _table WHERE key = 2 SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
ALTER TABLE t_repl_05228 DELETE WHERE _database != '' AND key = 3 SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;

SELECT key, value LIKE 'all_%' FROM t_repl_05228 WHERE key = 1;
SELECT key, value FROM t_repl_05228 WHERE key = 2;
SELECT count() FROM t_repl_05228;

DROP TABLE t_repl_05228 SYNC;

-- `_sample_factor` is declared non-deterministic because of `SELECT ... SAMPLE`, but no `SAMPLE` reaches a
-- mutation, so it is 1 on every replica and the guard accepts it.
DROP TABLE IF EXISTS t_repl_05228_sample SYNC;
CREATE TABLE t_repl_05228_sample (key Int, value Float64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05228/t_sample', 'r1') ORDER BY key;
INSERT INTO t_repl_05228_sample (key) VALUES (1), (2);
ALTER TABLE t_repl_05228_sample UPDATE value = _sample_factor WHERE key = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_repl_05228_sample DELETE WHERE _sample_factor = 1 AND key = 2 SETTINGS mutations_sync = 2;
SELECT key, value FROM t_repl_05228_sample ORDER BY key;
DROP TABLE t_repl_05228_sample SYNC;

-- An `ALIAS` or `EPHEMERAL` column over a non-deterministic virtual column is computed on read, so reading it
-- in a mutation reads the virtual column too.
DROP TABLE IF EXISTS t_repl_05228_alias SYNC;
CREATE TABLE t_repl_05228_alias (key Int, value String, v String ALIAS _table, w String ALIAS concat(v, 'x'), e String EPHEMERAL _database)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05228/t_alias', 'r1') ORDER BY key;
INSERT INTO t_repl_05228_alias (key, value) VALUES (1, 'a'), (2, 'b');
ALTER TABLE t_repl_05228_alias DELETE WHERE v != ''; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228_alias UPDATE value = w WHERE key = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228_alias DELETE WHERE t_repl_05228_alias.v != ''; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228_alias DELETE WHERE e != ''; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_repl_05228_alias DELETE WHERE v = 't_repl_05228_alias' AND key = 2 SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
SELECT key, value FROM t_repl_05228_alias ORDER BY key;
DROP TABLE t_repl_05228_alias SYNC;

-- A compound identifier naming a real subcolumn is not the virtual column, even when its first part
-- matches the table name.
DROP TABLE IF EXISTS b_05228 SYNC;
CREATE TABLE b_05228 (key Int, b_05228 Tuple(_table String))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05228/b', 'r1') ORDER BY key;
INSERT INTO b_05228 VALUES (1, tuple('x')), (2, tuple(''));
ALTER TABLE b_05228 DELETE WHERE b_05228._table != '' SETTINGS mutations_sync = 2;
SELECT key FROM b_05228 ORDER BY key;
DROP TABLE b_05228 SYNC;
