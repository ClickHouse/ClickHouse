-- The name-based form of the `remote` / `remoteSecure` / `cluster` / `clusterAllReplicas` table functions
-- resolves an empty database argument against the current database of the querying session at read time
-- (`evaluateConstantExpressionForDatabaseName`), so a persisted `Distributed` target freezes it to the
-- current database at CREATE time, like the other table-function targets
-- (04621_distributed_over_table_function_2). The binding walks the target recursively, so a nested
-- `remote` inside a scalar subquery in the target's argument is bound too - without that, create-time
-- structure inference evaluates the subquery on a copy and the persisted AST would keep the
-- session-dependent empty database argument. Verified by querying from a session whose current database
-- is a different one holding a decoy `bind_src` of the same name with different data; `SHOW CREATE`
-- locks in the persisted qualified form, and the DETACH/ATTACH round-trip from the other database checks
-- that re-running the normalization on the already-qualified persisted form is a no-op.
CREATE TABLE bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO bind_src VALUES (1), (2), (3);
CREATE TABLE dist_remote ENGINE = Distributed(test_shard_localhost, remote('127.0.0.1', '', 'bind_src'));
CREATE TABLE dist_cluster ENGINE = Distributed(test_shard_localhost, cluster('test_shard_localhost', '', 'bind_src'));
CREATE TABLE dist_nested ENGINE = Distributed(test_shard_localhost, numbers(assumeNotNull((SELECT count() FROM remote('127.0.0.1', '', 'bind_src')))));
SHOW CREATE TABLE dist_remote;
SHOW CREATE TABLE dist_cluster;
SHOW CREATE TABLE dist_nested;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bind_src VALUES (100), (200), (300), (400), (500);
USE {CLICKHOUSE_DATABASE_1:Identifier};
-- 6, not 1500: the empty database argument was bound to the creating database, not the querying one.
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_remote SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_remote SETTINGS enable_analyzer = 0;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_cluster SETTINGS enable_analyzer = 1;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_cluster SETTINGS enable_analyzer = 0;
-- 3 (numbers of the creating database's row count), not 5: the nested `remote` was bound too (analyzer
-- only, as the legacy path cannot evaluate the scalar subquery at read time).
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.dist_nested SETTINGS enable_analyzer = 1;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_remote;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.dist_remote;
SELECT sum(n) FROM {CLICKHOUSE_DATABASE:Identifier}.dist_remote;
USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE dist_nested;
DROP TABLE dist_cluster;
DROP TABLE dist_remote;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bind_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE bind_src;
