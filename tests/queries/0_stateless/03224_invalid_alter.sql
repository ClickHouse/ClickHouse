CREATE TABLE test
(
    str String,
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = MergeTree()
ORDER BY tuple();

-- The columns are named the same intentionally. It can catch an issue with
ALTER TABLE test ADD COLUMN invalid_column String MATERIALIZED concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test ADD COLUMN invalid_column String DEFAULT concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
-- alias is defined exactly the same
ALTER TABLE test ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
-- different alias
ALTER TABLE test ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
-- do one insert to make sure we can insert into the table
INSERT INTO test(str) VALUES ('test');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test;
DROP TABLE test;

CREATE TABLE test2
(
    str String,
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree('/clickhouse/03224_invalid_alter/{database}/{table}', 'r1')
ORDER BY tuple();

ALTER TABLE test2 ADD COLUMN invalid_column String MATERIALIZED concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test2 ADD COLUMN invalid_column String DEFAULT concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test2 ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
ALTER TABLE test2 ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
INSERT INTO test2(str) VALUES ('test2');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test2;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ON CLUSTER test_shard_localhost ENGINE = Atomic;

CREATE TABLE test3 ON CLUSTER test_shard_localhost
(
    str String,
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree('/clickhouse/03224_invalid_alter/{database}_atomic/{table}', 'r1')
ORDER BY tuple();

ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN invalid_column String MATERIALIZED concat(str, 'b' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN invalid_column String DEFAULT concat(str, 'b' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a);
ALTER TABLE test3 ON CLUSTER test_shard_localhost ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c);
INSERT INTO test3(str) VALUES ('test3');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test3;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Replicated('/clickhouse/03224_invalid_alter/{database}_replicated', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE test4
(
    str String,
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree()
ORDER BY tuple()
FORMAT Null;

ALTER TABLE test4 ADD COLUMN invalid_column String MATERIALIZED concat(str, 'b' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test4 ADD COLUMN invalid_column String DEFAULT concat(str, 'b' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test4 ADD COLUMN valid_column_1 String DEFAULT concat(str, 'a' AS a) FORMAT Null SETTINGS distributed_ddl_output_mode='throw';
ALTER TABLE test4 ADD COLUMN valid_column_2 String MATERIALIZED concat(str, 'c' AS c) FORMAT Null SETTINGS distributed_ddl_output_mode='throw';
INSERT INTO test4(str) VALUES ('test4');
SELECT str, column_with_alias, valid_column_1, valid_column_2 FROM test4;
