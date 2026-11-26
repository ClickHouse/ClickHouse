-- Regression test: `parseComment` must roll back `pos` when `COMMENT` is consumed but the
-- following string literal fails to parse, instead of silently accepting a misparsed query.
DROP VIEW IF EXISTS v_04613;
DROP DATABASE IF EXISTS db_04613;

-- Error case, `s_as` path: `COMMENT` before `AS` with no string literal.
-- With the bug the view was created without a comment; with the fix it is a syntax error.
CREATE VIEW v_04613 COMMENT AS SELECT 1; -- { clientError SYNTAX_ERROR }
SELECT count() FROM system.tables WHERE name = 'v_04613' AND database = currentDatabase();

-- Error case, end-of-query path: `COMMENT` at the end with no string literal.
CREATE DATABASE db_04613 COMMENT; -- { clientError SYNTAX_ERROR }
SELECT count() FROM system.databases WHERE name = 'db_04613';

-- Positive control: a well-formed `COMMENT 'text'` must still attach the comment (no over-rollback).
CREATE VIEW v_04613 COMMENT 'ok' AS SELECT 1;
SELECT comment FROM system.tables WHERE name = 'v_04613' AND database = currentDatabase();

DROP VIEW IF EXISTS v_04613;
DROP DATABASE IF EXISTS db_04613;
