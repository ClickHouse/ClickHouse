-- Regression test: CREATE TABLE AS SELECT ... INTERSECT SELECT ... SETTINGS ...
-- In debug builds, the AST formatting consistency check would trigger a
-- "Logical error: Inconsistent AST formatting" exception because the trailing
-- SETTINGS clause migrated from the ASTCreateQuery to the last ASTSelectQuery
-- during a format-reparse roundtrip.

DROP TABLE IF EXISTS t_intersect_settings;

-- Parenthesized SELECTs with trailing SETTINGS on CREATE (this was the failing case)
CREATE TABLE t_intersect_settings ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0;
DROP TABLE IF EXISTS t_intersect_settings;

-- Parenthesized SELECTs with both inner and outer SETTINGS
CREATE TABLE t_intersect_settings ENGINE = Null() AS (SELECT 1 AS x SETTINGS max_threads = 1) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0;
DROP TABLE IF EXISTS t_intersect_settings;

-- UNION ALL with trailing SETTINGS
CREATE TABLE t_intersect_settings ENGINE = Null() AS (SELECT 1 AS x) UNION ALL (SELECT 2 AS x) SETTINGS enable_global_with_statement = 0;
DROP TABLE IF EXISTS t_intersect_settings;

-- Verify formatting roundtrip preserves SETTINGS on the CREATE level
SELECT formatQuery('CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0');

-- Verify formatting roundtrip is idempotent
SELECT formatQuery('CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0')
     = formatQuery(formatQuery('CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0'));

-- Without trailing SETTINGS (should not add parens)
SELECT formatQuery('CREATE TABLE t ENGINE = Null() AS SELECT 1 AS x INTERSECT SELECT 1 AS x');
