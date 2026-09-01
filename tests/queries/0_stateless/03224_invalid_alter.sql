-- Tags: no-replicated-database
-- no-replicated-database: It messes up the output and this test explicitly checks the replicated database

CREATE TABLE test
(
    str String,
    column_with_codec String CODEC(ZSTD),
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = MergeTree()
ORDER BY tuple();

-- Cannot specify codec for column type ALIAS
ALTER TABLE test MODIFY COLUMN column_with_codec String ALIAS str; -- { serverError BAD_ARGUMENTS }
-- alias is defined exactly the same
ALTER TABLE test ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
-- different alias
ALTER TABLE test ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
-- do one insert to make sure we can insert into the table
INSERT INTO test(str, column_with_codec) VALUES ('test', 'test2');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test;
DROP TABLE test;

CREATE TABLE test2
(
    str String,
    column_with_codec String CODEC(ZSTD),
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree('/clickhouse/03224_invalid_alter/{database}/{table}', 'r1')
ORDER BY tuple();

ALTER TABLE test2 MODIFY COLUMN column_with_codec String ALIAS str; -- { serverError BAD_ARGUMENTS }
ALTER TABLE test2 ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
ALTER TABLE test2 ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
INSERT INTO test2(str, column_with_codec) VALUES ('test2', 'test22');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test2;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ON CLUSTER test_shard_localhost ENGINE = Atomic;

CREATE TABLE test3 ON CLUSTER test_shard_localhost
(
    str String,
    column_with_codec String CODEC(ZSTD),
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree('/clickhouse/03224_invalid_alter/{database}_atomic/{table}', 'r1')
ORDER BY tuple();

ALTER TABLE test3 ON CLUSTER test_shard_localhost MODIFY COLUMN column_with_codec String ALIAS str FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
INSERT INTO test3(str, column_with_codec) VALUES ('test3', 'test32');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test3;

-- ignore_drop_queries_probability = 0: the stress runner sets it to 0.2, which makes a DROP a no-op.
DROP TABLE test3 SETTINGS ignore_drop_queries_probability = 0;

-- {CLICKHOUSE_DATABASE} must not be re-engined: with --database it is shared by every test in
-- the client. {CLICKHOUSE_DATABASE_2} is shared too, so both DROPs are required.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/clickhouse/03224_invalid_alter/{database}_replicated', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.test4
(
    str String,
    column_with_codec String CODEC(ZSTD),
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree()
ORDER BY tuple()
FORMAT Null;

ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.test4 MODIFY COLUMN column_with_codec String ALIAS str FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.test4 ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw';
ALTER TABLE {CLICKHOUSE_DATABASE_2:Identifier}.test4 ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c) FORMAT Null SETTINGS distributed_ddl_output_mode='throw';
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.test4(str, column_with_codec) VALUES ('test4', 'test42');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM {CLICKHOUSE_DATABASE_2:Identifier}.test4;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

-- The engine varies with the runner flags, so assert only what must hold: this test must never
-- leave the database it was given Replicated.
SELECT engine = 'Replicated' FROM system.databases WHERE name = currentDatabase();
