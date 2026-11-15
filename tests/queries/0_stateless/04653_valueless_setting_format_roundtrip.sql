-- `SET name` with no value stands for `SET name = true` and is rejected for a setting that is not
-- Bool. That rejection depends on the AST remembering how the change was written, so formatting has
-- to keep the valueless form: written back as `name = true` the query would be accepted again.

SELECT '-- A valueless setting is formatted without a value';
SELECT formatQuery('SET max_threads');
SELECT formatQuery('SET optimize_on_insert');
SELECT formatQuerySingleLine('SELECT 1 SETTINGS max_threads');

SELECT '-- Formatting it is idempotent';
SELECT formatQuery(formatQuery('SET max_threads'));

SELECT '-- An explicit value is still formatted with it';
SELECT formatQuery('SET max_threads = 4');
SELECT formatQuery('SET optimize_on_insert = true');
SELECT formatQuerySingleLine('SELECT 1 SETTINGS optimize_on_insert = true');
