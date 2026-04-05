-- Regression test for inconsistent AST formatting when CREATE/VIEW/MATERIALIZED VIEW
-- uses AS SELECT ... INTERSECT/EXCEPT/UNION with trailing SETTINGS, FORMAT, or INTO OUTFILE.
--
-- The root cause was that formatQueryImpl for ASTCreateQuery did not wrap the
-- AS-select in parentheses when trailing output options existed (SETTINGS, FORMAT,
-- INTO OUTFILE). During re-parsing, ParserSelectQuery consumed the trailing clause
-- as part of the last SELECT in the UNION/INTERSECT chain, breaking the roundtrip.
--
-- Fixed by wrapping the AS-select in parentheses when settings_ast or
-- has_trailing_output_options is set.

SET max_execution_time = 30;

-- Helper: verify formatQuery roundtrip is idempotent for each query
-- (formatQuery(q) == formatQuery(formatQuery(q)))

-- 1. CREATE TABLE with INTERSECT + SETTINGS (the original failing case)
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 2. CREATE TABLE with UNION ALL + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) UNION ALL (SELECT 2 AS x) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 3. CREATE TABLE with UNION DISTINCT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS SELECT 1 UNION DISTINCT SELECT 2 SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 4. CREATE TABLE with EXCEPT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1) EXCEPT (SELECT 2) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 5. CREATE VIEW with INTERSECT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE VIEW v AS (SELECT 1) INTERSECT (SELECT 2) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 6. CREATE VIEW with UNION ALL + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE VIEW v AS (SELECT 1) UNION ALL (SELECT 2) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 7. CREATE MATERIALIZED VIEW with INTERSECT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE MATERIALIZED VIEW v TO t AS (SELECT 1) INTERSECT (SELECT 2) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 8. CREATE TABLE with FORMAT (trailing output option)
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1) INTERSECT (SELECT 2) FORMAT JSON' AS q)
ORDER BY q;

-- 9. CREATE TABLE with FORMAT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1) INTERSECT (SELECT 2) FORMAT JSON SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 10. CREATE TABLE with INTO OUTFILE (trailing output option)
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1) INTERSECT (SELECT 2) INTO OUTFILE ''/tmp/ast_test''' AS q)
ORDER BY q;

-- 11. EXPLAIN wrapping a CREATE TABLE with INTERSECT + SETTINGS
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'EXPLAIN AST CREATE TABLE t ENGINE = Null() AS (SELECT 1) INTERSECT (SELECT 2) SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 12. CREATE TABLE with multiple SETTINGS values
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS (SELECT 1) INTERSECT (SELECT 2) SETTINGS max_threads = 1, enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 13. Without trailing SETTINGS — should NOT add parens (no regression)
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS SELECT 1 INTERSECT SELECT 2' AS q)
ORDER BY q;

-- 14. Single SELECT (no UNION/INTERSECT) with SETTINGS — still must roundtrip
SELECT formatQuery(q) = formatQuery(formatQuery(q))
FROM (SELECT 'CREATE TABLE t ENGINE = Null() AS SELECT 1 SETTINGS enable_global_with_statement = 0' AS q)
ORDER BY q;

-- 15. Verify the actual formatted output includes wrapping parens when SETTINGS present
SELECT formatQuery('CREATE TABLE t ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0');

-- 16. Verify no parens added when SETTINGS absent
SELECT formatQuery('CREATE TABLE t ENGINE = Null() AS SELECT 1 AS x INTERSECT SELECT 1 AS x');

-- 17. Functional test: the CREATE TABLE actually works with INTERSECT + SETTINGS
DROP TABLE IF EXISTS t_ast_roundtrip;
CREATE TABLE t_ast_roundtrip ENGINE = Null() AS (SELECT 1 AS x) INTERSECT (SELECT 1 AS x) SETTINGS enable_global_with_statement = 0;
DROP TABLE IF EXISTS t_ast_roundtrip;

-- 18. Functional test: CREATE TABLE with UNION ALL + SETTINGS
DROP TABLE IF EXISTS t_ast_roundtrip;
CREATE TABLE t_ast_roundtrip ENGINE = Null() AS (SELECT 1 AS x) UNION ALL (SELECT 2 AS x) SETTINGS enable_global_with_statement = 0;
DROP TABLE IF EXISTS t_ast_roundtrip;