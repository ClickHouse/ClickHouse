-- A mutation is interpreted in a background thread, where the current database is not set.
-- The database has to be substituted into the `merge` table function when the mutation is created,
-- the same way as it is done for table names.

DROP TABLE IF EXISTS t_merge_mutation;
DROP TABLE IF EXISTS t_merge_mutation_src;

CREATE TABLE t_merge_mutation (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_mutation VALUES (1), (2), (3);

CREATE TABLE t_merge_mutation_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_merge_mutation_src VALUES (2);

ALTER TABLE t_merge_mutation DELETE WHERE id IN (SELECT id FROM merge('^t_merge_mutation_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM t_merge_mutation ORDER BY id;

-- The database of the mutated table is saved in the mutation command.
SELECT position(command, concat('merge(''', currentDatabase(), ''', ''^t_merge_mutation_src$'')')) > 0
FROM system.mutations WHERE database = currentDatabase() AND table = 't_merge_mutation';

-- The same for a lightweight delete.
INSERT INTO t_merge_mutation_src VALUES (3);
DELETE FROM t_merge_mutation WHERE id IN (SELECT id FROM merge('^t_merge_mutation_src$'));
SELECT id FROM t_merge_mutation ORDER BY id;

-- `currentDatabase()` in the arguments of the `merge` table function is substituted as well,
-- so the persisted mutation does not depend on the current database of the background thread.
INSERT INTO t_merge_mutation VALUES (5), (6);
INSERT INTO t_merge_mutation_src VALUES (5);
ALTER TABLE t_merge_mutation DELETE WHERE id IN (SELECT id FROM merge(currentDatabase(), '^t_merge_mutation_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM t_merge_mutation ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_merge_mutation' AND command LIKE '%currentDatabase%';

-- The database argument of the `merge` table function can be an arbitrary constant expression,
-- e.g. `concat(currentDatabase(), '')`, so `currentDatabase()` is substituted everywhere in it.
INSERT INTO t_merge_mutation VALUES (7), (8);
INSERT INTO t_merge_mutation_src VALUES (7);
ALTER TABLE t_merge_mutation DELETE WHERE id IN (SELECT id FROM merge(concat(currentDatabase(), ''), '^t_merge_mutation_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM t_merge_mutation ORDER BY id;

-- The aliases of `currentDatabase` are substituted as well.
INSERT INTO t_merge_mutation_src VALUES (8);
ALTER TABLE t_merge_mutation DELETE WHERE id IN (SELECT id FROM merge(database(), '^t_merge_mutation_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM t_merge_mutation ORDER BY id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_merge_mutation' AND (command ILIKE '%currentDatabase%' OR command ILIKE '%database()%');

-- The database is substituted even when the `merge` table function is an argument of another table function.
-- A view canonicalizes its query with the same visitor, so it can be used to check the rewrite without a connection.
CREATE VIEW v_merge_mutation AS SELECT id FROM remote('127.0.0.1', merge('^t_merge_mutation_src$'));
SELECT create_table_query LIKE concat('%merge(''', currentDatabase(), ''', ''^t_merge_mutation_src$'')%')
FROM system.tables WHERE database = currentDatabase() AND name = 'v_merge_mutation';
DROP TABLE v_merge_mutation;

-- The database of the mutated table is used, not the current database of the session.
CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation VALUES (1), (2), (3);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation_src VALUES (3);

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation
    DELETE WHERE id IN (SELECT id FROM merge('^t_merge_mutation_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation ORDER BY id;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE t_merge_mutation;
DROP TABLE t_merge_mutation_src;
