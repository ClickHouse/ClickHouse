SELECT '--- Alter commands in parens';
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr GROUP BY some_key), (ADD COLUMN a Int64)');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr TO VOLUME \'vol1\', expr2 + INTERVAL 2 YEAR TO VOLUME \'vol2\'), (DROP COLUMN c)');

SELECT '--- TTL expressions in parens';
SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), (expr2)');
SELECT formatQuery('ALTER TABLE a MODIFY TTL (expr GROUP BY some_key), MATERIALIZE TTL');

SELECT '--- Check only consistent parens around alter commands are accepted';
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), DROP COLUMN c'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, (DROP COLUMN c)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), (DROP COLUMN c)');
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, DROP COLUMN c'); -- Make sure it is backward compatible

SELECT '--- Check only consistent parens around TTL expressions are accepted';
-- This is a tricky one. We not supposed to allow inconsistent parens among TTL expressions, however "(expr2)" is a
-- valid expression on its own, thus the parens doesn't belong to the TTL expression, but to the expression, same as
-- it would like "SELECT (expr2)".
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (expr2))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (((expr2))))');
-- Almost same as above, but "expr2 GROUP BY expr3" cannot be parsed as an expression, thus the parens has to be
-- parsed as part of the TTL expression which results in inconsistent parens.
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), expr2)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, (expr2 GROUP BY expr3))'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), expr2 GROUP BY expr3)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), (expr2))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, expr2)'); -- Make sure it is backward compatible
SELECT formatQuery('ALTER TABLE a (MODIFY TTL (expr), (expr2 GROUP BY expr3))');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr, expr2 GROUP BY expr3)'); -- Make sure it is backward compatible
