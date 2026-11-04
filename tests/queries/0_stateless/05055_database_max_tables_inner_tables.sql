-- Tests that the `max_tables` database setting accounts for the hidden inner tables that a
-- `MATERIALIZED VIEW` creates. The whole group has to be reserved before the inner table is
-- created, otherwise the inner table is created first and the outer view is then rejected by the
-- quota, leaving the inner table behind.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;
USE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Only one slot is free, but a view with an inner table needs two.
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x
    AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src; -- { serverError TOO_MANY_TABLES }

-- The rejected view left nothing behind: only `src` is there.
SELECT count() FROM system.tables WHERE database = currentDatabase();

-- A view with an explicit `TO` table takes a single slot.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 3;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.dst
    AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
SELECT count() FROM system.tables WHERE database = currentDatabase();

-- Two free slots are enough for a view with an inner table.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 5;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv2 ENGINE = MergeTree ORDER BY x
    AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
SELECT count() FROM system.tables WHERE database = currentDatabase();

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
