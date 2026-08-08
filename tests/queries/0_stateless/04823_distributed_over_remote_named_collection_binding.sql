-- In the named-collection form of `remote` / `remoteSecure` the database is named by a
-- `database = ...` / `db = ...` override, and `parseRemoteFunctionArguments` resolves it through
-- `evaluateConstantExpressionForDatabaseName` at read time, falling back to the current database of the
-- querying session when it evaluates to an empty string. So a persisted `Distributed` target folds the
-- override to a literal and binds an empty result to the current database of the CREATE, exactly like
-- the positional database argument (04817_distributed_over_remote_table_function_binding). Verified by
-- querying from a session whose current database is a different one holding a decoy `bind_src` of the
-- same name with different data; `SHOW CREATE` locks in the persisted qualified form, and the
-- DETACH/ATTACH round-trip from the other database checks that re-running the normalization on the
-- already-qualified persisted form is a no-op.
DROP NAMED COLLECTION IF EXISTS nc_04823_binding;
CREATE NAMED COLLECTION nc_04823_binding AS addresses_expr = '127.0.0.1', table = 'unused';
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_nc_database ENGINE = Distributed(test_shard_localhost, remote(nc_04823_binding, database = '', table = 'bind_src'));
CREATE TABLE dist_nc_db ENGINE = Distributed(test_shard_localhost, remote(nc_04823_binding, db = '', table = 'bind_src'));
CREATE TABLE dist_nc_expression ENGINE = Distributed(test_shard_localhost, remote(nc_04823_binding, database = currentDatabase(), table = 'bind_src'));
SHOW CREATE TABLE dist_nc_database;
SHOW CREATE TABLE dist_nc_db;
SHOW CREATE TABLE dist_nc_expression;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_src VALUES (100), (200), (300), (400), (500);
USE {CLICKHOUSE_DATABASE_1:Identifier};
-- 6, not 1500: the empty database override was bound to the creating database, not the querying one.
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_database SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_database SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_db SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_db SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_expression SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_expression SETTINGS enable_analyzer = 0;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_nc_database;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_nc_database;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nc_database;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_nc_expression;
DROP TABLE dist_nc_db;
DROP TABLE dist_nc_database;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE bind_src;
DROP NAMED COLLECTION nc_04823_binding;
