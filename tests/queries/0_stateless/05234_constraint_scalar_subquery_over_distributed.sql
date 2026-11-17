-- The tables live in a Memory database because its table metadata is never written to disk: a `CHECK`
-- constraint reading a table function cannot be analysed while a database loads, so a leftover from an
-- interrupted run would stop the server starting. Cleanup is one `DROP DATABASE`, the one drop a stress
-- run never ignores; the fuzzer pin keeps this file's own DDL from being replayed under another name.
SET ast_fuzzer_any_query = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_scalar_constraint_source (id UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_scalar_constraint_source VALUES (1);

-- The scalar subquery of a CHECK constraint is executed while the table is created.
CREATE TABLE t_scalar_constraint_user (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM remote('127.0.0.1', currentDatabase(), 't_scalar_constraint_source'))) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_scalar_constraint_user';

-- Reloading the metadata analyses the constraint again.
DETACH TABLE t_scalar_constraint_user;
ATTACH TABLE t_scalar_constraint_user;
SELECT countIf(create_table_query LIKE '%CONSTRAINT c CHECK%') FROM system.tables WHERE database = currentDatabase() AND name = 't_scalar_constraint_user';
-- Dropped right away rather than at the end of the test: a `remote(...)` subquery cannot be analysed
-- during the asynchronous database load (it needs a query context and there is none), so a server restart
-- while this table exists - as a stress test does - fails to load it.
DROP TABLE t_scalar_constraint_user;

-- The remote table is really read: a function inside the subquery throws on the value it returns.
CREATE TABLE t_scalar_constraint_probe (x UInt64, CONSTRAINT c CHECK x < (SELECT throwIf(max(id) = 1, 'the scalar subquery was executed') FROM remote('127.0.0.1', currentDatabase(), 't_scalar_constraint_source'))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- A TTL DELETE WHERE predicate is analysed through the same producer as a CHECK constraint. Unlike a
-- constraint, it is analysed with a context whose current database is not the one the statement runs
-- in, so the source has to be named explicitly.
CREATE TABLE t_scalar_constraint_dist AS t_scalar_constraint_source ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_scalar_constraint_source');
CREATE TABLE t_scalar_constraint_ttl (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT throwIf(max(id) = 1, 'the TTL scalar subquery was executed') FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_scalar_constraint_dist); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The same read, but over the network: the shard of this cluster is local, so `prefer_localhost_replica`
-- decides whether `RemoteQueryExecutor` is used at all. The context of a standalone expression never went
-- through `makeQueryContext`, so its client version is zero and the shard query used to be rejected.
CREATE TABLE t_scalar_constraint_ttl_remote (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY tuple() TTL d + INTERVAL 1 YEAR WHERE x < (SELECT throwIf(max(id) = 1, 'the remote TTL scalar subquery was executed') FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_scalar_constraint_dist SETTINGS prefer_localhost_replica = 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- A SETTINGS clause written inside the subquery must not re-enable parallel replicas either: the cluster
-- named below exists in no configuration, so the statement only survives while the setting stays off.
CREATE TABLE t_scalar_constraint_inner (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM t_scalar_constraint_source SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 2, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'no_such_cluster')) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_scalar_constraint_inner';
-- The clause the user wrote is still what the table metadata keeps.
SELECT countIf(create_table_query LIKE '%no_such_cluster%') FROM system.tables WHERE database = currentDatabase() AND name = 't_scalar_constraint_inner';

-- The subquery must not inherit the statement's parallel-replica settings either: the cluster named
-- below exists in no configuration, so the statement only survives while the inherited setting is off.
SET allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'no_such_cluster_session', parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;
CREATE TABLE t_scalar_constraint_pr (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM t_scalar_constraint_source)) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_scalar_constraint_pr';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
