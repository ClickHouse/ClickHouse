-- Tests the `max_tables` database setting. Per-database limit on the number of tables.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;
USE {CLICKHOUSE_DATABASE_1:Identifier};

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
SELECT count() FROM system.tables WHERE database = currentDatabase();
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

-- RENAME TABLE into the database is subject to the limit.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.moved (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.moved TO {CLICKHOUSE_DATABASE_1:Identifier}.moved; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = currentDatabase();
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} AND name = 'moved';

-- A failed UNDROP must leave metadata in the dropped-table queue.
-- The drop has to be asynchronous, otherwise there is nothing to undrop.
SET database_atomic_wait_for_drop_and_detach_synchronously = 0;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_source (x UInt32) ENGINE = MergeTree ORDER BY x;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_source;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_target (x UInt32) ENGINE = MergeTree ORDER BY x;
UNDROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_source; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.dropped_tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND table = 'undrop_source';
-- Freeing a slot makes the same `UNDROP` succeed.
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_target;
UNDROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.undrop_source;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'undrop_source';
-- `UNDROP` of a table that was never dropped reports it as unknown, even when the database is full.
UNDROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.no_such_table; -- { serverError UNKNOWN_TABLE }
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
SET database_atomic_wait_for_drop_and_detach_synchronously = 1;

-- The setting survives detaching and re-attaching the database.
USE {CLICKHOUSE_DATABASE:Identifier};
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t8 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

-- A materialized view's hidden inner table consumes a slot of its own.
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 7;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.t1;
SELECT count() FROM system.tables WHERE database = currentDatabase();
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t9 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }

USE {CLICKHOUSE_DATABASE:Identifier};
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

-- Dictionaries are table-like objects: they consume a slot and are restricted by the limit.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = 2;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dict_source (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict (x UInt64) PRIMARY KEY x SOURCE(CLICKHOUSE(TABLE 'dict_source')) LIFETIME(0) LAYOUT(FLAT());
-- The dictionary took the second slot, so neither a table nor another dictionary fits now.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dt2 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }
CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict2 (x UInt64) PRIMARY KEY x SOURCE(CLICKHOUSE(TABLE 'dict_source')) LIFETIME(0) LAYOUT(FLAT()); -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String};
-- Renaming a dictionary into a full database is blocked by the limit, and the dictionary stays put.
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.occupies_slot (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict TO {CLICKHOUSE_DATABASE_2:Identifier}.dict; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'dict';
-- Freeing a slot lets the same rename through.
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.occupies_slot;
RENAME DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.dict TO {CLICKHOUSE_DATABASE_2:Identifier}.dict;
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_2:String} AND name = 'dict';
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The limit is enforced by the Ordinary engine as well, and its setting can be altered too.
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.o1 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.o2 (x UInt32) ENGINE = MergeTree ORDER BY x; -- { serverError TOO_MANY_TABLES }
ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING max_tables = 5;
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT extract(engine_full, 'max_tables\\s*=\\s*(\\d+)') FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
-- A quota-rejected CREATE must not leave any on-disk state behind: the limit is checked before
-- the storage is constructed, so this DROP works without `force_remove_data_recursively_on_drop`.
USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- A full target must reject all cross-database rename paths before detaching the source table.
-- Both databases are per-test unique, so the test stays runnable in parallel with itself.
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Ordinary SETTINGS max_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.target (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ordinary_source (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ordinary_source TO {CLICKHOUSE_DATABASE_2:Identifier}.ordinary_source; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'ordinary_source';
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.atomic_source (x UInt32) ENGINE = MergeTree ORDER BY x;
RENAME TABLE {CLICKHOUSE_DATABASE:Identifier}.atomic_source TO {CLICKHOUSE_DATABASE_2:Identifier}.atomic_source; -- { serverError TOO_MANY_TABLES }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE:String} AND name = 'atomic_source';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_tables = -1; -- { serverError CANNOT_CONVERT_TYPE }
