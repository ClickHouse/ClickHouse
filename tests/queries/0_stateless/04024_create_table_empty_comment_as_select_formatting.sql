-- Test that CREATE TABLE/VIEW with EMPTY and COMMENT accepts both orderings
-- and formats consistently (EMPTY before COMMENT) for AST roundtrip.

-- Table: COMMENT then EMPTY (original parser order)
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' EMPTY AS SELECT 1');

-- Table: EMPTY then COMMENT (formatter output order)
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c EMPTY COMMENT \'test\' AS SELECT 1');

-- Table: EMPTY without COMMENT
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c EMPTY AS SELECT 1');

-- Table: COMMENT without EMPTY
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' AS SELECT 1');

-- Materialized view: EMPTY then COMMENT
SELECT formatQuery('CREATE MATERIALIZED VIEW v (c Int8) ENGINE = MergeTree ORDER BY c EMPTY COMMENT \'test\' AS SELECT 1');

-- Materialized view: COMMENT then EMPTY
SELECT formatQuery('CREATE MATERIALIZED VIEW v (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' EMPTY AS SELECT 1');
