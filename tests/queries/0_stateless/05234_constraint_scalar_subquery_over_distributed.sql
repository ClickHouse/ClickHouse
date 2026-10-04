-- The tables live in a Memory database because its table metadata is never written to disk: a scalar
-- subquery reading a table function cannot be analysed while a database loads, so a leftover from an
-- interrupted run would stop the server starting. Cleanup is one `DROP DATABASE`, the one drop a stress
-- run never ignores; the fuzzer pin keeps this file's own DDL from being replayed under another name.
SET ast_fuzzer_any_query = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_scalar_constraint_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_scalar_constraint_source VALUES (1);

-- A scalar subquery is not allowed in a CHECK constraint, so it is rejected before anything reads the
-- remote table.
CREATE TABLE t_scalar_constraint_user (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM remote('127.0.0.1', currentDatabase(), 't_scalar_constraint_source'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The subquery is not executed: the error is the validation one, not the one of `throwIf`.
CREATE TABLE t_scalar_constraint_probe (x UInt64, CONSTRAINT c CHECK x < (SELECT throwIf(max(id) = 1, 'the scalar subquery was executed') FROM remote('127.0.0.1', currentDatabase(), 't_scalar_constraint_source'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A TTL DELETE WHERE predicate does allow a scalar subquery, and it is executed while the table is
-- created. It is analysed with a context whose current database is not the one the statement runs in,
-- so the source has to be named explicitly.
CREATE TABLE t_scalar_constraint_dist AS t_scalar_constraint_source ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_scalar_constraint_source');
CREATE TABLE t_scalar_constraint_ttl (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT throwIf(max(id) = 1, 'the TTL scalar subquery was executed') FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_scalar_constraint_dist); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The same read, but over the network: the shard of this cluster is local, so `prefer_localhost_replica`
-- decides whether `RemoteQueryExecutor` is used at all. The context of a standalone expression never went
-- through `makeQueryContext`, so its client version is zero and the shard query used to be rejected.
CREATE TABLE t_scalar_constraint_ttl_remote (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT throwIf(max(id) = 1, 'the remote TTL scalar subquery was executed') FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_scalar_constraint_dist SETTINGS prefer_localhost_replica = 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
