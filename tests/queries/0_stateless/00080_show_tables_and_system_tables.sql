-- Tags: log-engine

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.A (A UInt8) ENGINE = TinyLog;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.B (A UInt8) ENGINE = TinyLog;

SHOW TABLES FROM {CLICKHOUSE_DATABASE:Identifier};
SHOW TABLES IN system WHERE engine LIKE '%System%' AND name IN ('numbers', 'one') AND database = 'system';

SELECT name, toUInt32(metadata_modification_time) > 0, engine_full, create_table_query FROM system.tables WHERE database = currentDatabase() ORDER BY name FORMAT TSVRaw;

CREATE TEMPORARY TABLE test_temporary_table (id UInt64);
SELECT name FROM system.tables WHERE is_temporary = 1 AND name = 'test_temporary_table';

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.test_log(id UInt64) ENGINE = Log;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE:Identifier}.test_materialized ENGINE = Log AS SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.test_log;
SELECT dependencies_database, dependencies_table FROM system.tables WHERE name = 'test_log' AND database=currentDatabase();

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};

-- Check that create_table_query works for system tables and unusual Databases
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.A (A UInt8) ENGINE = Null;

SELECT sum(ignore(*, metadata_modification_time, engine_full, create_table_query)) FROM system.tables WHERE database = '{CLICKHOUSE_DATABASE:String}';
