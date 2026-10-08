-- Tags: no-replicated-database
-- Regression test: ALTER TABLE ... MODIFY QUERY with SETTINGS must not produce
-- extra parentheses in nested subqueries that break re-parsing (inconsistent AST formatting).

DROP TABLE IF EXISTS test_view_04046;
DROP TABLE IF EXISTS test_table_04046;

CREATE TABLE test_table_04046 (c0 String, c1 String, c2 String) ENGINE = MergeTree ORDER BY c0;

CREATE MATERIALIZED VIEW test_view_04046 TO test_table_04046 AS SELECT c0, c1, c2 FROM test_table_04046;

-- The key reproducer: ALTER TABLE with MODIFY QUERY containing a subquery with SETTINGS,
-- plus trailing SETTINGS on the ALTER itself.
-- This used to produce double-parenthesized subqueries like ((SELECT ... SETTINGS ...))
-- which failed to re-parse.
ALTER TABLE test_view_04046 (MODIFY QUERY SELECT c0 FROM (SELECT c0 FROM test_table_04046 SETTINGS max_threads = 1)), (MODIFY COMMENT 'test') SETTINGS mutations_sync = 2;

SELECT 'OK';

DROP TABLE test_view_04046;
DROP TABLE test_table_04046;
