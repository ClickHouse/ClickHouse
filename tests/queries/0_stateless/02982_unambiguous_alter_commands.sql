SELECT '--- Alter commands in parens';
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr GROUP BY some_key), (ADD COLUMN a Int64)');
SELECT formatQuery('ALTER TABLE a (MODIFY TTL expr TO VOLUME \'vol1\', expr2 + INTERVAL 2 YEAR TO VOLUME \'vol2\'), (DROP COLUMN c)');

SELECT '--- Check only consistent parens around alter commands are accepted';
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), DROP COLUMN c'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, (DROP COLUMN c)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('ALTER TABLE a (DROP COLUMN b), (DROP COLUMN c)');
SELECT formatQuery('ALTER TABLE a DROP COLUMN b, DROP COLUMN c'); -- Make sure it is backward compatible
