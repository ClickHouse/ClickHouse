-- The create-time binding of a table-function target must also replace the SQL-standard aliases of
-- `currentDatabase` - `DATABASE()`, `SCHEMA()`, and `current_database()` (registered case-insensitively) -
-- with the creating database, not only a function spelled literally `currentDatabase`. The aliases are
-- placed where only the generic DDL normalization handles them (inside a scalar subquery in the
-- function's argument): the database argument positions of `remote` / `merge` are frozen separately by
-- evaluating them to a literal, so they would not catch a regression in the generic pass. Verified by
-- querying from a session whose current database is a different one: without the substitution the alias
-- would be evaluated at read time under the querying session and pick the decoy row. The reads are
-- pinned to the analyzer, because the legacy path cannot evaluate a scalar subquery in a table function
-- argument at read time (see 04817_distributed_over_remote_table_function_binding). The DETACH/ATTACH
-- round trip checks that re-running the normalization on the already-bound persisted form is a no-op.
CREATE TABLE alias_src (d String, v UInt64) ENGINE = MergeTree ORDER BY d;
INSERT INTO alias_src VALUES ({CLICKHOUSE_DATABASE:String}, 6), ({CLICKHOUSE_DATABASE_1:String}, 1500);
CREATE TABLE dist_database ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT v FROM alias_src WHERE d = DATABASE()))));
CREATE TABLE dist_schema ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT v FROM alias_src WHERE d = SCHEMA()))));
CREATE TABLE dist_lower ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT v FROM alias_src WHERE d = current_database()))));
CREATE TABLE remote_database (number UInt64) ENGINE = Remote('127.0.0.1', numbers(assumeNotNull((SELECT v FROM alias_src WHERE d = DATABASE()))));
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name LIKE 'dist_%' ORDER BY name;
SELECT replaceAll(engine_full, currentDatabase(), '_db_') FROM system.tables WHERE database = currentDatabase() AND name = 'remote_database';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
-- 6, not 1500: every alias was bound to the creating database at CREATE time.
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_database SETTINGS enable_analyzer = 1;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_schema SETTINGS enable_analyzer = 1;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_lower SETTINGS enable_analyzer = 1;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.remote_database SETTINGS enable_analyzer = 1;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_database;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_database;
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_database SETTINGS enable_analyzer = 1;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE remote_database;
DROP TABLE dist_lower;
DROP TABLE dist_schema;
DROP TABLE dist_database;
DROP TABLE alias_src;
