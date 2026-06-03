-- Tags: zookeeper, no-replicated-database, no-ordinary-database, need-query-parameters
-- no-replicated-database: we explicitly run this test by creating a replicated database

SET allow_experimental_alias_table_engine = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT NULL;

-- Create Replicated database (using unique path based on database name)
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/test/{database}_repl_alias_alter', 'shard1', 'replica1') FORMAT NULL;

-- Create target table in Replicated database
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.target_table (c0 Int32, c1 String) ENGINE = Null FORMAT NULL;

-- Create alias table in the SAME Replicated database (for Test 2)
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_table_same_db ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, target_table) FORMAT NULL;

-- Create alias table pointing to target from a different database (for Test 1)
-- Using default database (CLICKHOUSE_DATABASE) for Atomic database
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.alias_table_cross_db ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, target_table) FORMAT NULL;

-- =============================================================================
-- Test 1: Cross-database ALTER (alias in Atomic, target in Replicated)
-- This should fail because DDL worker path is bypassed
-- =============================================================================
ALTER TABLE {CLICKHOUSE_DATABASE:Identifier}.alias_table_cross_db ADD COLUMN cross_db_col Int32; -- { serverError NOT_IMPLEMENTED }

-- =============================================================================
-- Test 2: Same-database ALTER (both alias and target in same Replicated database)
-- This should work because DDL worker handles the ALTER correctly
-- =============================================================================
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_table_same_db ADD COLUMN same_db_col Int32 FORMAT NULL;

-- Verify that the column WAS added (DESCRIBE TABLE output should include same_db_col)
DESCRIBE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.target_table;

-- Cleanup
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.alias_table_cross_db FORMAT NULL;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.alias_table_same_db FORMAT NULL;
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier}.target_table FORMAT NULL;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT NULL;
