-- Tags: distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- The database of the mutated table has to be substituted into the `merge` table function
-- before the query is written to the distributed DDL queue, so a clustered mutation reads
-- the same tables as a non-clustered one, even when the database of the session is different.

SET distributed_ddl_output_mode = 'none';

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation VALUES (1), (2), (3);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation_src VALUES (2);

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation ON CLUSTER test_shard_localhost
    DELETE WHERE id IN (SELECT id FROM merge('^t_merge_mutation_src$'))
    SETTINGS mutations_sync = 2;

SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation ORDER BY id;

-- `currentDatabase()` is substituted with the database of the mutated table, not with the database of the session.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation VALUES (5), (6);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation_src VALUES (5);

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation ON CLUSTER test_shard_localhost
    DELETE WHERE id IN (SELECT id FROM merge(currentDatabase(), '^t_merge_mutation_src$'))
    SETTINGS mutations_sync = 2;

SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_merge_mutation ORDER BY id;

-- The persisted mutation commands contain the database of the mutated table as a literal.
SELECT
    countIf(command LIKE concat('%merge(''', {CLICKHOUSE_DATABASE_1:String}, ''', ''^t_merge_mutation_src$'')%')),
    countIf(command ILIKE '%currentDatabase%')
FROM system.mutations WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_merge_mutation';

-- The mutations belong to the database of the mutated table, not to the database of the session.
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_merge_mutation';

-- An `UPDATE ON CLUSTER` substitutes the database of the updated table as well. The source table
-- exists in the database of the session with a different row, so a table function or a table
-- identifier resolved there updates a different row.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu VALUES (1, 0), (2, 0), (3, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu_src VALUES (2);
CREATE TABLE t_lwu_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lwu_src VALUES (3);

-- The explicitly qualified form gives the same answer with and without the substitution.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu ON CLUSTER test_shard_localhost
    SET v = 1 WHERE id IN (SELECT id FROM merge({CLICKHOUSE_DATABASE_1:String}, '^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 1;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu ON CLUSTER test_shard_localhost
    SET v = 2 WHERE id IN (SELECT id FROM merge('^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 2;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu ON CLUSTER test_shard_localhost
    SET v = 3 WHERE id IN (SELECT id FROM t_lwu_src);
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 3;

-- `currentDatabase()` inside a table function is substituted on the initiator, before the query
-- reaches the distributed DDL queue: the host cannot recover it, because the queue rewrite has
-- already replaced it with the database of the session.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu ON CLUSTER test_shard_localhost
    SET v = 4 WHERE id IN (SELECT id FROM merge(currentDatabase(), '^t_lwu_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE v = 4;

-- The expressions of the assignments are substituted as well, not only the predicate. The row at
-- `id = 2` holds 4 from the arm above, so the read-back also fails if the statement does not update
-- the row at all.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu ON CLUSTER test_shard_localhost
    SET v = (SELECT max(id) FROM merge(currentDatabase(), '^t_lwu_src$')) + 30 WHERE id = 2;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_lwu WHERE id = 2;

DROP TABLE t_lwu_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
