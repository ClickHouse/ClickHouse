-- Tags: no-parallel
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104864
-- Follow-up to the GRANT fix: row policies must also reject prefix wildcards
-- like db*.* or table* because RowPolicyName cannot represent them.

-- ============================================================
-- PART 1: Valid patterns -- must still work correctly
-- ============================================================

DROP ROW POLICY IF EXISTS policy_04657 ON mydb.mytable;
DROP ROW POLICY IF EXISTS policy_04657 ON mydb.*;
DROP ROW POLICY IF EXISTS policy_04657 ON *.*;
DROP ROW POLICY IF EXISTS policy_04657 ON mytable;

-- Valid: specific db, specific table
CREATE ROW POLICY policy_04657 ON mydb.mytable AS PERMISSIVE FOR SELECT USING 1 TO default;
SHOW POLICIES ON mydb.mytable;
SHOW CREATE POLICY policy_04657 ON mydb.mytable;
DROP ROW POLICY policy_04657 ON mydb.mytable;

-- Valid: specific db, all tables
CREATE ROW POLICY policy_04657 ON mydb.* AS PERMISSIVE FOR SELECT USING 1 TO default;
SHOW POLICIES ON mydb.*;
SHOW CREATE POLICY policy_04657 ON mydb.*;
DROP ROW POLICY policy_04657 ON mydb.*;

-- Valid: all dbs, all tables
CREATE ROW POLICY policy_04657 ON *.* AS PERMISSIVE FOR SELECT USING 1 TO default;
SHOW POLICIES ON *.*;
SHOW CREATE POLICY policy_04657 ON *.*;
DROP ROW POLICY policy_04657 ON *.*;

-- Valid: table in default database
CREATE ROW POLICY policy_04657 ON mytable AS PERMISSIVE FOR SELECT USING 1 TO default;
SHOW POLICIES ON mytable;
SHOW CREATE POLICY policy_04657 ON mytable;
DROP ROW POLICY policy_04657 ON mytable;

-- ============================================================
-- PART 2: Invalid patterns -- must produce SYNTAX_ERROR
-- ============================================================

-- Database prefix wildcard + all tables: RowPolicyName cannot represent db*.*
CREATE ROW POLICY policy_04657 ON mydb*.* AS PERMISSIVE FOR SELECT USING 1 TO default; -- { clientError SYNTAX_ERROR }

-- Database prefix wildcard + specific table: also invalid
CREATE ROW POLICY policy_04657 ON mydb*.mytable AS PERMISSIVE FOR SELECT USING 1 TO default; -- { clientError SYNTAX_ERROR }

-- Database prefix wildcard + table prefix wildcard: also invalid
CREATE ROW POLICY policy_04657 ON mydb*.mytable* AS PERMISSIVE FOR SELECT USING 1 TO default; -- { clientError SYNTAX_ERROR }

-- Table prefix wildcard: RowPolicyName cannot represent table*
CREATE ROW POLICY policy_04657 ON mydb.mytable* AS PERMISSIVE FOR SELECT USING 1 TO default; -- { clientError SYNTAX_ERROR }

-- Table prefix wildcard in default database: also invalid
CREATE ROW POLICY policy_04657 ON mytable* AS PERMISSIVE FOR SELECT USING 1 TO default; -- { clientError SYNTAX_ERROR }

-- ALTER ROW POLICY with invalid patterns
ALTER ROW POLICY IF EXISTS policy_04657 ON mydb*.* TO default; -- { clientError SYNTAX_ERROR }
ALTER ROW POLICY IF EXISTS policy_04657 ON mytable* TO default; -- { clientError SYNTAX_ERROR }

-- DROP ROW POLICY with invalid patterns
DROP ROW POLICY IF EXISTS policy_04657 ON mydb*.*; -- { clientError SYNTAX_ERROR }
DROP ROW POLICY IF EXISTS policy_04657 ON mytable*; -- { clientError SYNTAX_ERROR }

-- SHOW POLICIES with invalid patterns
SHOW POLICIES ON mydb*.*; -- { clientError SYNTAX_ERROR }
SHOW POLICIES ON mytable*; -- { clientError SYNTAX_ERROR }

-- SHOW CREATE POLICY with invalid patterns
SHOW CREATE POLICY policy_04657 ON mydb*.*; -- { clientError SYNTAX_ERROR }
SHOW CREATE POLICY policy_04657 ON mytable*; -- { clientError SYNTAX_ERROR }
