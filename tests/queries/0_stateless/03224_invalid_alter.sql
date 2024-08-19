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

CREATE TABLE test2
(
    str String,
    column_with_alias String MATERIALIZED concat(str, 'a' AS a),
)
ENGINE = ReplicatedMergeTree('/clickhouse/03224_invalid_alter/{database}/{table}', 'r1')
ORDER BY tuple();

ALTER TABLE test2 ADD COLUMN invalid_column String MATERIALIZED concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
ALTER TABLE test2 ADD COLUMN invalid_column String DEFAULT concat(str, 'b' AS a); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }


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
