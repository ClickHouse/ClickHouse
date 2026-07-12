-- A Distributed table can be created over a table function, like the `cluster`/`remote` table functions.

DROP TABLE IF EXISTS dist_over_tf;
DROP TABLE IF EXISTS dist_over_tf_local;

-- The structure is inferred from the table function; single (local) shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT sum(number), count() FROM dist_over_tf;
-- The table function survives a metadata round-trip (it is re-parsed on ATTACH).
DETACH TABLE dist_over_tf;
ATTACH TABLE dist_over_tf;
SELECT sum(number), count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Two shards: the table function is executed on every shard.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- Explicit columns and an optional sharding key are accepted.
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
SELECT count() FROM dist_over_tf;
DROP TABLE dist_over_tf;

-- A second argument that is not a registered table function is still treated as a database name,
-- so the classic `Distributed(cluster, database, table)` form (including `currentDatabase()`) is unaffected.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
INSERT INTO dist_over_tf_local VALUES (1), (2), (3);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
SELECT sum(x) FROM dist_over_tf;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- INSERT into a table-function-backed Distributed table is rejected (there is no concrete remote table).
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf VALUES (100); -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- Too many arguments for the table-function form.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10), number, 'default', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A `*Cluster` table function cannot back a table (`ITableFunctionCluster::canBeUsedToCreateTable` is false),
-- so it is rejected at create time, exactly as `CREATE TABLE ... AS urlCluster(...)` is - even when the
-- columns are given explicitly (otherwise the unsupported combination would only surface later at read time).
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE dist_over_tf (x String) ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
-- A user-issued `ATTACH TABLE ... (columns) ENGINE = ...` is a fresh query (`LoadingStrictnessLevel::ATTACH`),
-- not a load from previously-validated metadata, so it must be rejected too - otherwise it re-opens the bug
-- through a different entrypoint. Only server-startup / force-restore loads skip the check. (A `Memory`
-- database accepts `ATTACH TABLE` with an explicit definition without a UUID, unlike an `Atomic` one.)
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dist_over_tf (x String) ENGINE = Distributed(test_shard_localhost, urlCluster('test_shard_localhost', 'http://x/y', 'CSV')); -- { serverError BAD_ARGUMENTS }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A table function that resolves back to the Distributed table itself recurses, but the recursion is bounded
-- by `max_distributed_depth` (it does not hang): reading raises `TOO_LARGE_DISTRIBUTED_DEPTH`, the same way two
-- classic `Distributed` tables that reference each other do (self-references are only detected at create time
-- for the direct `Distributed(cluster, database, table)` form).
CREATE TABLE dist_over_tf (x UInt8) ENGINE = Distributed(test_shard_localhost, merge(currentDatabase(), '^dist_over_tf$'));
SELECT * FROM dist_over_tf SETTINGS max_distributed_depth = 3; -- { serverError TOO_LARGE_DISTRIBUTED_DEPTH }
DROP TABLE dist_over_tf;

-- A `dictGet` in the sharding key registers a loading dependency on the dictionary, so on restart the
-- Distributed table is loaded after the dictionary it references. In the table-function form the sharding key
-- is the 3rd argument (vs the 4th in the classic form), so the loading-dependency visitor must look at the
-- shifted position - otherwise the dependency is silently dropped and the table can fail to load after a reboot.
DROP DICTIONARY IF EXISTS shard_dict;
CREATE DICTIONARY shard_dict (key UInt64, val UInt64)
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY 'SELECT 0 AS key, 0 AS val'))
LAYOUT(FLAT())
LIFETIME(0);
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_shard_localhost, numbers(10), dictGetUInt64('shard_dict', 'val', number));
SELECT loading_dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'dist_over_tf';
DROP TABLE dist_over_tf;
DROP DICTIONARY shard_dict;

-- `additional_table_filters` matched against the Distributed table cannot be propagated to the shards
-- when the target is a table function: the shard query reads from the table function, which has no named
-- source table to re-key the filter onto (its shard-side expression is referenced only by an internally
-- generated alias). It is rejected with a clear error instead of the confusing `UNKNOWN_TABLE`
-- ("Both table name and UUID are empty") that `main_table.getShortName()` produced on the empty source id.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT count() FROM dist_over_tf SETTINGS additional_table_filters = {'dist_over_tf': 'number > 5'}; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- The classic named-table form still supports `additional_table_filters` (the filter is re-keyed onto the
-- source table and applied on the shards): `number > 5` keeps 4 of the 10 rows, summing to 30.
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
INSERT INTO dist_over_tf_local SELECT * FROM numbers(10);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
SELECT count(), sum(number) FROM dist_over_tf SETTINGS additional_table_filters = {'dist_over_tf': 'number > 5'};
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_local;

-- `INSERT ... SELECT` into a table-function-backed Distributed table is rejected with the same
-- `NOT_IMPLEMENTED` as the `INSERT ... VALUES` path, including with `parallel_distributed_insert_select`
-- enabled (its distributed fast paths would otherwise build an `INSERT` into an empty remote table id).
CREATE TABLE dist_over_tf_local (number UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_src ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf SELECT * FROM dist_over_tf_src SETTINGS parallel_distributed_insert_select = 2; -- { serverError NOT_IMPLEMENTED }
INSERT INTO dist_over_tf SELECT * FROM dist_over_tf_src SETTINGS parallel_distributed_insert_select = 1; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_src;
DROP TABLE dist_over_tf_local;

-- `INSERT ... SELECT` *from* a table-function-backed Distributed table with
-- `parallel_distributed_insert_select` keeps the projection and the filter of the original query:
-- the query shipped to the shards is rebuilt from the original `SELECT` with the table replaced by
-- the (aliased) table function, the same way it is done for a named-table source - not replaced with
-- a bare `SELECT * FROM table_function()`, which would silently drop them and write wrong rows.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst SELECT dist_over_tf.number + 1 FROM dist_over_tf WHERE number < 5 SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
-- The same holds for a `view` source (whose inner `SELECT` used to replace the whole query) read
-- through the `cluster` table function, which shares this code path.
TRUNCATE TABLE dist_over_tf_local;
INSERT INTO FUNCTION cluster('test_shard_localhost', currentDatabase(), dist_over_tf_local)
SELECT v.number * 2 FROM cluster('test_shard_localhost', view(SELECT number FROM numbers(10))) AS v WHERE v.number >= 8
SETTINGS parallel_distributed_insert_select = 2;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- The projection/filter preservation must also work for a database-qualified source reference
-- (`db.dist_over_tf.number`): the shard-side rewrite restores the qualified name onto the table-function
-- alias, the same way the legacy read path does, otherwise `db.dist_over_tf.number` would dangle against
-- `numbers(10) AS dist_over_tf` on the shard and fail to resolve.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number + 1
FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf
WHERE number < 5
SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- The qualified-asterisk form (`db.dist_over_tf.*`) must be rewritten onto the shard-side alias in the
-- parallel `INSERT ... SELECT` fast path too: it is an `ASTQualifiedAsterisk`, whose qualifier is a whole
-- table reference rather than an `ASTIdentifier` column, so the database qualifier would otherwise dangle
-- against `numbers(10) AS dist_over_tf` on the shard and the fast path would fail at execution time.
CREATE TABLE dist_over_tf_local (x UInt64) ENGINE = Memory;
CREATE TABLE dist_over_tf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_over_tf_local);
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
INSERT INTO dist_over_tf_dst
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.*
FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf
SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1;
SELECT count(), sum(x) FROM dist_over_tf_dst;
DROP TABLE dist_over_tf;
DROP TABLE dist_over_tf_dst;
DROP TABLE dist_over_tf_local;

-- `distributed_product_mode = 'local'` rewrites a nested Distributed subquery to its concrete remote table.
-- A table-function-backed Distributed table has no such table, so the rewrite is rejected with a clear
-- `NOT_IMPLEMENTED` instead of failing deep inside the rewrite on an empty table id. Covered for both the
-- analyzer (`buildQueryTreeForShard`) and the old analyzer (`InJoinSubqueriesPreprocessor`); needs >= 2 shards.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;

-- Qualified column references against a table-function-backed Distributed table must resolve on the shard
-- on the legacy (enable_analyzer = 0) read path too. That path replaces the `FROM` clause with the table
-- function, so the table function is aliased with the same qualifier the original query used for the
-- Distributed table - its name, or an explicit alias - and qualified references (including the
-- database-qualified `db.table.column` form) are restored onto that alias, otherwise they would
-- dangle on the shard. (The analyzer path resolves columns structurally; it is covered as well.)
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(10));
SELECT sum(dist_over_tf.number) FROM dist_over_tf SETTINGS enable_analyzer = 0;
SELECT sum(d.number) FROM dist_over_tf AS d SETTINGS enable_analyzer = 0;
SELECT sum({CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 0;
SELECT sum(dist_over_tf.number) FROM dist_over_tf SETTINGS enable_analyzer = 1;
SELECT sum(d.number) FROM dist_over_tf AS d SETTINGS enable_analyzer = 1;
SELECT sum({CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.number) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf SETTINGS enable_analyzer = 1;
DROP TABLE dist_over_tf;

-- The qualified-*asterisk* form (`db.dist_over_tf.*`) is an `ASTQualifiedAsterisk`, whose qualifier is a
-- whole table reference rather than an `ASTIdentifier` column, so `RestoreQualifiedNamesVisitor` (which only
-- rewrites identifiers) leaves it untouched and `db.dist_over_tf.*` dangles against `numbers(...) AS
-- dist_over_tf` on the shard. The asterisk qualifier must be rewritten onto the alias as well. Covered for the
-- table-qualified, alias-qualified and database-qualified forms on the legacy path (and the analyzer path,
-- which resolves it structurally, as a control). A three-row source keeps the reference small.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_shard_localhost, numbers(3));
SELECT dist_over_tf.* FROM dist_over_tf ORDER BY number SETTINGS enable_analyzer = 0;
SELECT d.* FROM dist_over_tf AS d ORDER BY number SETTINGS enable_analyzer = 0;
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.* FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf ORDER BY number SETTINGS enable_analyzer = 0;
SELECT {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf.* FROM {CLICKHOUSE_DATABASE:Identifier}.dist_over_tf ORDER BY number SETTINGS enable_analyzer = 1;
DROP TABLE dist_over_tf;

-- A `sharding_key` passed to the table-function form is not a real shard map: every shard runs the same
-- table function and returns the same rows, so the key must not drive read optimizations. With
-- `optimize_skip_unused_shards`, `WHERE number = 1` must still query every shard (one matching row per shard,
-- i.e. 2, not 1), and with `optimize_distributed_group_by_sharding_key` the final cross-shard merge must
-- still run (a single `1 2` group, not a `1 1` finalized per shard). Covered for both analyzers.
CREATE TABLE dist_over_tf (number UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10), number);
SELECT count() FROM dist_over_tf WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 1;
SELECT count() FROM dist_over_tf WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 0;
SELECT number, count() FROM dist_over_tf WHERE number = 1 GROUP BY number ORDER BY number SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, enable_analyzer = 1;
SELECT number, count() FROM dist_over_tf WHERE number = 1 GROUP BY number ORDER BY number SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1, enable_analyzer = 0;
DROP TABLE dist_over_tf;

-- The disable above is scoped to the persisted `Distributed(...)` engine over a table function. The
-- `remote`/`cluster`/`clusterAllReplicas` table functions also build a `Distributed` over a table function,
-- but they own their cluster and have always used a sharding key for shard skipping - a long-standing feature
-- (see `01930_optimize_skip_unused_shards_rewrite_in`, `01952_optimize_distributed_group_by_sharding_key`) -
-- so that must keep working. Here `optimize_skip_unused_shards` still prunes `WHERE number = 1` to a single
-- shard and returns 1, not 2. Covered for both analyzers.
SELECT count() FROM cluster('test_cluster_two_shards_localhost', numbers(10), number) WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 1;
SELECT count() FROM cluster('test_cluster_two_shards_localhost', numbers(10), number) WHERE number = 1 SETTINGS optimize_skip_unused_shards = 1, enable_analyzer = 0;
