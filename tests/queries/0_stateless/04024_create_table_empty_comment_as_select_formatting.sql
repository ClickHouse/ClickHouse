-- Test that CREATE TABLE ... EMPTY COMMENT ... AS SELECT formats correctly for AST roundtrip.
-- The parser expects COMMENT before EMPTY AS for tables, and EMPTY before COMMENT for views.

-- Table with EMPTY AS SELECT and COMMENT: parser expects COMMENT then EMPTY AS
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' EMPTY AS SELECT 1');

-- Table with EMPTY AS SELECT, no COMMENT
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c EMPTY AS SELECT 1');

-- Table with COMMENT, no EMPTY
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' AS SELECT 1');

-- Materialized view with EMPTY and COMMENT
SELECT formatQuery('CREATE MATERIALIZED VIEW v (c Int8) ENGINE = MergeTree ORDER BY c EMPTY COMMENT \'test\' AS SELECT 1');
