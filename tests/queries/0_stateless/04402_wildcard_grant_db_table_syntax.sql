-- Tags: no-parallel
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104864
-- parseDatabaseAndTableNameOrAsterisks silently converted db*.table into db.table*

-- ============================================================
-- PART 1: Valid wildcard patterns -- must still work correctly
-- ============================================================

DROP USER IF EXISTS user_04402;
CREATE USER user_04402;

-- Valid: specific db, all tables
GRANT SELECT ON db.* TO user_04402;
SHOW GRANTS FOR user_04402;

-- Valid: db prefix wildcard, all tables
GRANT SELECT ON mydb*.* TO user_04402;
SHOW GRANTS FOR user_04402;

-- Valid: specific db, table prefix wildcard
GRANT SELECT ON db.mytable* TO user_04402;
SHOW GRANTS FOR user_04402;

-- Valid: all dbs, all tables
GRANT SELECT ON *.* TO user_04402;
SHOW GRANTS FOR user_04402;

DROP USER user_04402;

-- ============================================================
-- PART 2: Invalid patterns -- must produce SYNTAX_ERROR
-- ============================================================

-- BUG WAS HERE: db*.table was silently interpreted as db.table*
-- Now it must be rejected as invalid/ambiguous syntax
GRANT SELECT ON db*.table TO user_04402; -- { clientError SYNTAX_ERROR }

-- db wildcard + specific table name = also invalid
GRANT SELECT ON db*.mytable TO user_04402; -- { clientError SYNTAX_ERROR }

-- db wildcard + table wildcard combined = also invalid
GRANT SELECT ON mydb*.mytable* TO user_04402; -- { clientError SYNTAX_ERROR }
