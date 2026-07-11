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

-- `distributed_product_mode = 'local'` rewrites a nested Distributed subquery to its concrete remote table.
-- A table-function-backed Distributed table has no such table, so the rewrite is rejected with a clear
-- `NOT_IMPLEMENTED` instead of failing deep inside the rewrite on an empty table id. Covered for both the
-- analyzer (`buildQueryTreeForShard`) and the old analyzer (`InJoinSubqueriesPreprocessor`); needs >= 2 shards.
CREATE TABLE dist_over_tf ENGINE = Distributed(test_cluster_two_shards_localhost, numbers(10));
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM dist_over_tf WHERE number IN (SELECT number FROM dist_over_tf) SETTINGS distributed_product_mode = 'local', enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
DROP TABLE dist_over_tf;
