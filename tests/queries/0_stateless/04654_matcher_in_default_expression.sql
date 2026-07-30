-- The matcher guard lives on the analyzer path only, so this SET is load-bearing:
-- without it the old analyzer reports MULTIPLE_EXPRESSIONS_FOR_ALIAS instead. A session
-- SET also holds under the stress `compatibility` randomization, which a tag would not.
SET enable_analyzer = 1;

-- Every cell wraps the matcher in the duplicate-alias shape from the issue, so a build
-- without the guard reports MULTIPLE_EXPRESSIONS_FOR_ALIAS while a build with it reports
-- UNKNOWN_IDENTIFIER. A plain matcher reports UNKNOWN_IDENTIFIER either way.

SELECT 'asterisk';
CREATE TABLE t_asterisk (c0 Int DEFAULT tuple(1 AS a0, * + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'qualified asterisk';
CREATE TABLE t_qualified_asterisk (c0 Int DEFAULT tuple(1 AS a0, c2.* + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'columns regexp';
CREATE TABLE t_columns_regexp (c0 Int DEFAULT tuple(1 AS a0, COLUMNS('x.*') + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'columns list';
CREATE TABLE t_columns_list (a Int, b Int, c0 Int DEFAULT tuple(1 AS a0, COLUMNS(a, b) + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'qualified columns regexp';
CREATE TABLE t_qualified_columns_regexp (c0 Int DEFAULT tuple(1 AS a0, c2.COLUMNS('x.*') + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 'qualified columns list';
CREATE TABLE t_qualified_columns_list (a Int, b Int, c0 Int DEFAULT tuple(1 AS a0, c2.COLUMNS(a, b) + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

-- A matcher-free default must still be accepted, so that a change rejecting every default
-- expression reddens differently from the guard firing.
SELECT 'no matcher';
CREATE TABLE t_no_matcher (a Int, c0 Int DEFAULT a + 2) ENGINE = Memory;
