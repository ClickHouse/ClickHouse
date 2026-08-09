-- Tags: no-parallel
-- no-parallel: CREATE/DROP NAMED COLLECTION mutate global server state shared by concurrent tests
-- (see 02918_fuzzjson_table_function.sql for the same requirement), and the flaky check runs this very
-- test concurrently with itself, so one run's DROP NAMED COLLECTION would remove the collection from
-- under another run.

-- Without a `database` / `db` override in the AST, the named-collection form of `remote` /
-- `remoteSecure` reads the database from the collection itself, and an empty value stored there falls
-- back to the current database of the querying session at read time - just as session-dependent as an
-- empty `database = ''` override (04823_distributed_over_remote_named_collection_binding). So a persisted
-- `Distributed` target resolves the collection at CREATE time and freezes an empty stored database by
-- injecting a literal `database = ...` override with the current database of the CREATE; `SHOW CREATE`
-- locks in the injected override, and reading from a session in a different database (holding a decoy
-- `bind_src` of the same name with different data) proves the binding. A non-empty stored database is
-- deliberately left in the collection (following later edits of the collection is the point of the
-- indirection), so no override is injected for it.
DROP NAMED COLLECTION IF EXISTS nc_04826_empty_db;
DROP NAMED COLLECTION IF EXISTS nc_04826_fixed_db;
CREATE NAMED COLLECTION nc_04826_empty_db AS addresses_expr = '127.0.0.1', database = '', table = 'bind_src';
CREATE NAMED COLLECTION nc_04826_fixed_db AS addresses_expr = '127.0.0.1', database = 'system', table = 'one';
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_nc_empty_db ENGINE = Distributed(test_shard_localhost, remote(nc_04826_empty_db));
CREATE TABLE dist_nc_table_override ENGINE = Distributed(test_shard_localhost, remote(nc_04826_empty_db, table = 'bind_src'));
CREATE TABLE dist_nc_fixed_db ENGINE = Distributed(test_shard_localhost, remote(nc_04826_fixed_db));
SHOW CREATE TABLE dist_nc_empty_db;
SHOW CREATE TABLE dist_nc_table_override;
SHOW CREATE TABLE dist_nc_fixed_db;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_src VALUES (100), (200), (300), (400), (500);
USE {CLICKHOUSE_DATABASE_1:Identifier};
-- 6, not 1500: the empty database stored in the collection was bound to the creating database, not the
-- querying one.
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_table_override SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_table_override SETTINGS enable_analyzer = 0;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_fixed_db;
-- Re-running the normalization on the already-injected persisted form is a no-op: the override is
-- there, so nothing is injected twice.
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db;
SHOW CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_empty_db;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_nc_fixed_db;
DROP TABLE dist_nc_table_override;
DROP TABLE dist_nc_empty_db;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE bind_src;
DROP NAMED COLLECTION nc_04826_fixed_db;
DROP NAMED COLLECTION nc_04826_empty_db;
