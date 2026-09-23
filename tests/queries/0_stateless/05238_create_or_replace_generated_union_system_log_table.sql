-- Tags: zookeeper, no-ordinary-database
-- Tag no-ordinary-database: CREATE OR REPLACE is not supported for the Ordinary database engine

-- A union system log table (`system.all_query_log` and friends, see `create_union_system_log_tables`) is a
-- data-free proxy that the server generates and replaces whenever its definition is outdated. Its definition
-- declares that in its comment, and `CREATE OR REPLACE` of such a definition may only replace another
-- generated union table of the same log: a user's table that sits on the name is never dropped. The rule is
-- decided from the query alone, so it also holds in a database that replays its DDL from the query text.

DROP TABLE IF EXISTS all_query_log;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- In the (Atomic) test database.
CREATE TABLE all_query_log (d Date, note String) ENGINE = MergeTree ORDER BY d;
INSERT INTO all_query_log SELECT '2026-01-01', 'precious' FROM numbers(10);

CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'Union of the `query_log` table and its rotated versions.\n\nIt is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }

SELECT count(), any(note) FROM all_query_log;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'all_query_log';

-- The same definition in the other spellings the table functions accept - bare identifiers, an expression for
-- the database, the database left out to mean the current one, the log table as one qualified name - is the
-- same definition: the table function rewrites its arguments into literals before the rule is applied.
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge('^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas(test_shard_localhost, merge('^query_log(_[0-9]+)?$'))
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:Identifier}, '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge(currentDatabase(), '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas(test_shard_localhost, {CLICKHOUSE_DATABASE:Identifier}, query_log)
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas(test_shard_localhost, {CLICKHOUSE_DATABASE:Identifier}.query_log)
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas('test_shard_localhost', concat({CLICKHOUSE_DATABASE:String}, '.query_log'), SETTINGS skip_unavailable_shards = 1)
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas('test_shard_localhost', merge(currentDatabase(), '^query_log(_[0-9]+)?$'))
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas(test_shard_localhost, merge({CLICKHOUSE_DATABASE:Identifier}, concat('^query_log', '(_[0-9]+)?$')), SETTINGS skip_unavailable_shards = 1)
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
SELECT count(), any(note) FROM all_query_log;

-- The user's own proxy over the same table function, and a copy of the comment over another table, are kept too.
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$');
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log_stale$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.';
CREATE OR REPLACE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
SELECT create_table_query LIKE '%recreated automatically.%' FROM system.tables WHERE database = currentDatabase() AND name = 'all_query_log';

-- Only the union table name of the log is managed by the server: the same definition under any other name,
-- including the union table name of another log, is an ordinary user table and replaces whatever is there.
DROP TABLE IF EXISTS report;
DROP TABLE IF EXISTS all_text_log;
CREATE TABLE report (x UInt8) ENGINE = Memory;
CREATE OR REPLACE TABLE report (x UInt8) AS merge(currentDatabase(), '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.';
SELECT create_table_query LIKE '%AS merge(%' FROM system.tables WHERE database = currentDatabase() AND name = 'report';
CREATE TABLE all_text_log (x UInt8) ENGINE = Memory;
CREATE OR REPLACE TABLE all_text_log (x UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.';
SELECT create_table_query LIKE '%AS merge(%' FROM system.tables WHERE database = currentDatabase() AND name = 'all_text_log';
DROP TABLE report;
DROP TABLE all_text_log;

-- A generated union table with a stale definition is replaced by an up-to-date one, in any of the generated shapes.
DROP TABLE all_query_log;
CREATE TABLE all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'Union of the `query_log` table and its rotated versions.\n\nIt is safe to drop this table at any time: it will be recreated automatically.';
CREATE OR REPLACE TABLE all_query_log (event_date Date, query String) AS clusterAllReplicas('test_shard_localhost', {CLICKHOUSE_DATABASE:String}, 'query_log')
    COMMENT 'Union of the `query_log` tables across all replicas of the cluster `test_shard_localhost`.\n\nIt is safe to drop this table at any time: it will be recreated automatically.';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'all_query_log' ORDER BY name;
CREATE OR REPLACE TABLE all_query_log (event_date Date) AS clusterAllReplicas(test_shard_localhost, {CLICKHOUSE_DATABASE:Identifier}.query_log)
    COMMENT 'Union of the `query_log` tables across all replicas of the cluster `test_shard_localhost`.\n\nIt is safe to drop this table at any time: it will be recreated automatically.';
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'all_query_log' ORDER BY name;
SELECT create_table_query LIKE '%clusterAllReplicas(''test_shard_localhost'', ''%.query_log'')%' FROM system.tables WHERE database = currentDatabase() AND name = 'all_query_log';

-- And a user may replace a generated union table with anything.
CREATE OR REPLACE TABLE all_query_log (x UInt64) ENGINE = MergeTree ORDER BY x;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'all_query_log';
DROP TABLE all_query_log;

-- In a Replicated database, where the query is replayed from its text by the DDL worker.
SET distributed_ddl_output_mode = 'none';
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/databases/{database}/generated_union_table', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (d Date, note String) ENGINE = ReplicatedMergeTree ORDER BY d;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log SELECT '2026-01-01', 'precious' FROM numbers(10);

CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE_1:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'Union of the `query_log` table and its rotated versions.\n\nIt is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }

CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE_1:Identifier}, '^query_log(_[0-9]+)?$')
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (event_date Date) AS clusterAllReplicas(test_shard_localhost, {CLICKHOUSE_DATABASE_1:Identifier}.query_log)
    COMMENT 'It is safe to drop this table at any time: it will be recreated automatically.'; -- { serverError TABLE_ALREADY_EXISTS }

SELECT count(), any(note) FROM {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log;
-- Not the exact engine name: it may be replaced by another engine of the `MergeTree` family.
SELECT engine LIKE '%MergeTree' FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'all_query_log';

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log SYNC;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (dummy UInt8) AS merge({CLICKHOUSE_DATABASE_1:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'Union of the `query_log` table and its rotated versions.\n\nIt is safe to drop this table at any time: it will be recreated automatically.';
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.all_query_log (event_date Date, query String) AS merge({CLICKHOUSE_DATABASE_1:String}, '^query_log(_[0-9]+)?$')
    COMMENT 'Union of the `query_log` table and its rotated versions.\n\nIt is safe to drop this table at any time: it will be recreated automatically.';
SELECT name FROM system.columns WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'all_query_log' ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
