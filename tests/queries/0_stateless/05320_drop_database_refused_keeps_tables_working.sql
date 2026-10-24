-- Tags: zookeeper, no-ordinary-database
-- A refused DROP DATABASE / TRUNCATE DATABASE must not drop any table, and must leave every table writable and mergeable.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE rmt (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', '1') ORDER BY id;
CREATE TABLE mt (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE src (key UInt64, value String) ENGINE = MergeTree ORDER BY key;
INSERT INTO rmt VALUES (1);
INSERT INTO mt VALUES (1);
INSERT INTO src VALUES (1, 'a');

-- A dictionary in another database depends on `src`.
CREATE DICTIONARY {CLICKHOUSE_DATABASE_2:Identifier}.dict (key UInt64, value String) PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LIFETIME(0) LAYOUT(FLAT());

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }
SELECT 'dependency', groupArray(name) FROM (SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name);
INSERT INTO rmt VALUES (2);
INSERT INTO mt VALUES (2);
OPTIMIZE TABLE mt FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE rmt FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
SELECT 'dependency', table, count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table IN ('mt', 'rmt') AND active GROUP BY table ORDER BY table;
SELECT 'dependency', is_readonly FROM system.replicas WHERE database = currentDatabase();
DROP DICTIONARY {CLICKHOUSE_DATABASE_2:Identifier}.dict;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SETTINGS max_table_size_to_drop = 1; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
SELECT 'size', groupArray(name) FROM (SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name);
INSERT INTO rmt VALUES (3);
INSERT INTO mt VALUES (3);
OPTIMIZE TABLE mt FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE rmt FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
SELECT 'size', table, count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table IN ('mt', 'rmt') AND active GROUP BY table ORDER BY table;
SELECT 'size', is_readonly FROM system.replicas WHERE database = currentDatabase();

TRUNCATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SETTINGS max_table_size_to_drop = 1; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
SELECT 'truncate', groupArray(name) FROM (SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name);
SELECT 'truncate', (SELECT count() FROM rmt), (SELECT count() FROM mt), (SELECT count() FROM src);
INSERT INTO rmt VALUES (4);
INSERT INTO mt VALUES (4);
OPTIMIZE TABLE mt FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE rmt FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
SELECT 'truncate', table, count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table IN ('mt', 'rmt') AND active GROUP BY table ORDER BY table;
SELECT 'truncate', is_readonly FROM system.replicas WHERE database = currentDatabase();

-- `ReplicatedMergeTree` does not support transactions.
BEGIN TRANSACTION;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SETTINGS throw_on_unsupported_query_inside_transaction = 0; -- { serverError NOT_IMPLEMENTED }
ROLLBACK;
SELECT 'transaction', groupArray(name) FROM (SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name);
INSERT INTO rmt VALUES (5);
INSERT INTO mt VALUES (5);
OPTIMIZE TABLE mt FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE rmt FINAL SETTINGS optimize_throw_if_noop = 1, alter_sync = 2;
SELECT 'transaction', table, count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table IN ('mt', 'rmt') AND active GROUP BY table ORDER BY table;
SELECT 'transaction', is_readonly FROM system.replicas WHERE database = currentDatabase();

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'dropped', count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
