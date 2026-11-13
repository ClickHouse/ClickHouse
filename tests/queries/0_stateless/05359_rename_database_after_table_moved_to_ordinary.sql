-- A table moved from an Atomic database to an Ordinary one is no longer detached in the source,
-- so the source can still be renamed.
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt64) ENGINE = Memory;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t TO {CLICKHOUSE_DATABASE_2:Identifier}.t;

SELECT count() FROM system.detached_tables WHERE database = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
SELECT engine FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_2:String};

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
