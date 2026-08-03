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
