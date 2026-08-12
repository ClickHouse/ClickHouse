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

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
