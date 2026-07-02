-- Tags: no-replicated-database
-- Test: exercises `ALTER ... MODIFY SQL SECURITY` definer-clearing path AND new `MODIFY DEFINER` standalone syntax
-- Covers: src/Storages/StorageInMemoryMetadata.cpp:117-118 — `else: definer = std::nullopt;` branch
-- Covers: src/Databases/DatabasesCommon.cpp:74-83 — applyMetadataChangesToCreateQuery rebuilds AST with cleared definer
-- Covers: src/Parsers/ParserAlterQuery.cpp — new MODIFY DEFINER keyword path (standalone, no SQL SECURITY clause)

DROP TABLE IF EXISTS test_v;
DROP TABLE IF EXISTS test_t;

CREATE TABLE test_t (s String) ENGINE = MergeTree ORDER BY s;

-- ============================================================================
-- Case 1: View with explicit DEFINER, then ALTER MODIFY SQL SECURITY INVOKER
-- The DEFINER must be cleared from the metadata (and from SHOW CREATE TABLE)
-- ============================================================================
CREATE VIEW test_v (s String) DEFINER = default SQL SECURITY DEFINER AS SELECT * FROM test_t;

-- Verify initial state has DEFINER
SELECT 'case1_initial_has_definer', countMatches(create_table_query, 'DEFINER = default') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';

ALTER TABLE test_v MODIFY SQL SECURITY INVOKER;

-- After ALTER MODIFY SQL SECURITY INVOKER, DEFINER must be gone
SELECT 'case1_after_alter_invoker_has_security', countMatches(create_table_query, 'SQL SECURITY INVOKER') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';
SELECT 'case1_after_alter_invoker_has_definer', countMatches(create_table_query, 'DEFINER = default') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';

DROP TABLE test_v;

-- ============================================================================
-- Case 2: ALTER ... MODIFY DEFINER = user (standalone — no SQL SECURITY clause)
-- This exercises the new `MODIFY DEFINER` parser keyword path. The implicit
-- SQL SECURITY DEFINER is added by ParserSQLSecurity when a definer is set
-- without an explicit type.
-- ============================================================================
CREATE VIEW test_v (s String) SQL SECURITY INVOKER AS SELECT * FROM test_t;

SELECT 'case2_initial_security', countMatches(create_table_query, 'SQL SECURITY INVOKER') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';
SELECT 'case2_initial_has_definer', countMatches(create_table_query, 'DEFINER = default') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';

ALTER TABLE test_v MODIFY DEFINER = default;

-- After ALTER MODIFY DEFINER = default (no SQL SECURITY clause):
-- - DEFINER must be set to default
-- - Type defaults to SQL SECURITY DEFINER
SELECT 'case2_after_modify_definer_alone_has_definer', countMatches(create_table_query, 'DEFINER = default') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';
SELECT 'case2_after_modify_definer_alone_has_security_definer', countMatches(create_table_query, 'SQL SECURITY DEFINER') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';
SELECT 'case2_after_modify_definer_alone_no_invoker', countMatches(create_table_query, 'SQL SECURITY INVOKER') FROM system.tables WHERE database = currentDatabase() AND name = 'test_v';

DROP TABLE test_v;
DROP TABLE test_t;
