-- Tags: long
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

-- An `UPDATE` and a lightweight `DELETE` use the database of the updated table as well, both in the
-- expression of the predicate and in the expressions of the assignments. The source table exists in
-- both databases with a different row, so an expression resolved in the database of the session
-- silently updates a different row instead of failing.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu VALUES (1, 0), (2, 0), (3, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu_src VALUES (2);
CREATE TABLE t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lwu_src VALUES (3);

-- The explicitly qualified forms give the same answer with and without the substitution.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = 1 WHERE id IN (SELECT id FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = 2 WHERE id IN (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu_src);
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 2;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = 3 WHERE id IN (SELECT id FROM merge('^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 3;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = 4 WHERE id IN (SELECT id FROM t_lwu_src);
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 4;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = 5 WHERE id IN (SELECT id FROM merge(currentDatabase(), '^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 5;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = (SELECT max(id) FROM merge('^t_lwu_src$')) WHERE id = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE id = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu SET v = (SELECT max(id) FROM t_lwu_src) WHERE id = 3;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE id = 3;

-- A lightweight `DELETE` is rewritten to an `UPDATE`, so it uses the same substitution.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd VALUES (1), (2), (3);

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd WHERE id IN (SELECT id FROM merge('^t_lwu_src$'))
    SETTINGS lightweight_delete_mode = 'lightweight_update';
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd ORDER BY id;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd_plain (id UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd_plain VALUES (1), (2), (3);

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd_plain WHERE id IN (SELECT id FROM t_lwu_src)
    SETTINGS lightweight_delete_mode = 'lightweight_update';
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwd_plain ORDER BY id;

-- A common table expression is expanded before the database is filled in, so its name is not
-- qualified as if it were a table, and the table it reads is resolved in the updated database.
-- The old analyzer resolves a common table expression in a subquery of a mutation as a table, so
-- the analyzer is requested explicitly in the arms that use one.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu
    SET v = 6 WHERE id IN (WITH c AS (SELECT id FROM t_lwu_src) SELECT id FROM c)
    SETTINGS enable_analyzer = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 6;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu
    SET v = (WITH c AS (SELECT max(id) AS m FROM t_lwu_src) SELECT m FROM c) WHERE id = 2
    SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE id = 2;

-- The name of a recursive common table expression in the predicate is not the name of a table, so it
-- does not hide a table of the same name in the assignments, which is resolved in the updated database.
-- The offset makes the correct value differ from the value the row already has, so the read-back
-- also fails if the statement does not update the row at all.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu
    SET v = (SELECT max(id) FROM t_lwu_src) + 20
    WHERE id IN (WITH RECURSIVE t_lwu_src AS (SELECT 3 AS id UNION ALL SELECT id + 1 FROM t_lwu_src WHERE id < 3) SELECT id FROM t_lwu_src)
    SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE id = 3;

DROP TABLE t_lwu_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE t_merge_mutation;
DROP TABLE t_merge_mutation_src;
