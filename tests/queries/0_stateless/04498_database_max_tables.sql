-- Tests the `max_tables` database setting. Per-database limit on the number of tables.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;

-- The setting is stored in the database metadata.
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- Up to the limit is allowed.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Exceeding the limit throws.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t3 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- Dropping a table frees a slot.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t3 (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Raising the limit lets more tables be created.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 3;
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t4 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t5 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- Lowering the limit does not drop existing tables, but blocks new ones.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 1;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t6 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- 0 means unlimited.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t6 (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Only `max_tables` can be altered on this engine.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING disk = 'default'; -- { serverError BAD_ARGUMENTS }

-- Non-integer or negative values are rejected with the same error as the equivalent SETTINGS clause at CREATE time.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = -1; -- { serverError CANNOT_CONVERT_TYPE }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 'oops'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- String literals that parse as non-negative integers are accepted.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = '5';
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

-- Fill up to the limit (tables so far: t1, t3, t4, t6; t7 brings us to 5).
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t7 (x UInt32) ENGINE = MergeTree ORDER BY x;

-- CREATE OR REPLACE briefly needs an extra slot for the replacement table, so at the limit it
-- throws even though the final table count would not grow (documented behavior).
CREATE OR REPLACE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t7 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- CREATE TABLE IF NOT EXISTS on an existing table is a no-op even at the limit.
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.t1 (x UInt32) ENGINE = MergeTree ORDER BY x;

-- RENAME TABLE into the database does not go through the table-creation path and bypasses the
-- limit (documented behavior).
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.moved (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.moved TO {CLICKHOUSE_DATABASE_1:Identifier}.moved;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};

-- The setting survives detaching and re-attaching the database.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t8 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- A materialized view's hidden inner table consumes a slot of its own.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 8;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.t1;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t9 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- ATTACH is subject to the limit, like CREATE.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.b (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Detaching frees a slot which a new table can take.
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.b;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.c (x UInt32) ENGINE = MergeTree ORDER BY x;

-- Re-attaching the detached table would exceed the limit.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.b; -- { serverError TOO_MANY_TABLES }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- UNDROP re-attaches a dropped table through the table-creation path, so it is subject to the limit.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ua (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ub (x UInt32) ENGINE = MergeTree ORDER BY x;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ua;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.uc (x UInt32) ENGINE = MergeTree ORDER BY x;
UNDROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ua; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The limit is enforced by the Ordinary engine as well, and its setting can be altered too.
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.o1 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.o2 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 5;
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
-- A rejected CREATE leaves a data directory behind on Ordinary, so drop the database recursively.
SET force_remove_data_recursively_on_drop = 1;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SET force_remove_data_recursively_on_drop = 0;

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = -1; -- { serverError CANNOT_CONVERT_TYPE }
