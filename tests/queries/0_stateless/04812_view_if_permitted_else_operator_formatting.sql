-- The ELSE argument of viewIfPermitted must keep the function-call form when formatted:
-- the parser accepts only a function call after ELSE, so formatting `not(...)` as the
-- `NOT` operator produced a query that could not be parsed back (inconsistent AST formatting).
SELECT formatQuerySingleLine('SELECT * FROM viewIfPermitted(SELECT 1 ELSE not(isNull(1)))');
-- The formatted query parses back and formats the same way.
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT * FROM viewIfPermitted(SELECT 1 ELSE not(isNull(1)))'));
-- Multi-line formatting takes the same code path.
SELECT formatQuery('SELECT * FROM viewIfPermitted(SELECT 1 ELSE not(isNull(1)))');
-- The normal case of a table function after ELSE is unaffected.
SELECT formatQuerySingleLine('SELECT * FROM viewIfPermitted(SELECT 1 AS x ELSE null(''x UInt8''))');
-- In an expression context viewIfPermitted parses as an ordinary function, so the ELSE form
-- (which only the table function parser accepts) must not be used for it: formatting
-- `viewIfPermitted(1, 2)` as `viewIfPermitted(1 ELSE 2)` could not be parsed back.
SELECT formatQuerySingleLine('SELECT viewIfPermitted(1, 2)');
SELECT formatQuerySingleLine(formatQuerySingleLine('SELECT viewIfPermitted(1, not(2))'));
