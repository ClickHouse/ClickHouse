-- Tests function named arguments: f(name => value)

SELECT '-- format';

SELECT formatQuery('SELECT f(1, `my arg` => 2, b=>3)');
SELECT formatQuery('SELECT f(a => x AS y)');
SELECT formatQuery('SELECT f(arr => [1], fn => x -> x + 1, g => (x, y) -> x)');
SELECT formatQuery('SELECT f(a => g(b => 1))');
SELECT formatQuery('SELECT f(a => 1, x -> x, b => y, z, w -> w)');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT f(b => x, a => \'y\')')) SETTINGS enable_json_ast_dialect = 1;
SELECT formatQuery('SELECT f(a => 1)(x)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT a => 1'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT f(a =>)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuery('SELECT f(1, a =>)'); -- { serverError SYNTAX_ERROR }

SELECT '-- errors';

SELECT concat(a => 'x'); -- { serverError BAD_ARGUMENTS }
SELECT sum(x => 1); -- { serverError BAD_ARGUMENTS }
SELECT * FROM numbers(n => 1); -- { serverError BAD_ARGUMENTS }
WITH x -> x AS f SELECT f(x => 1); -- { serverError BAD_ARGUMENTS }
DROP FUNCTION IF EXISTS f_named_arguments;
CREATE FUNCTION f_named_arguments AS (a, b) -> concat(b, a);
SELECT f_named_arguments(a => 'x', b => 'y'); -- { serverError BAD_ARGUMENTS }
DROP FUNCTION f_named_arguments;

SELECT '-- secrets are masked when arguments are named';

SELECT encrypt('aes-128-ecb', plaintext => 'text', key => 'named_secret_key_1'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/a.csv', access_key_id => 'x', secret_access_key => 'named_secret_key_2'); -- { serverError BAD_ARGUMENTS }
SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%named\_secret\_key\_%' AND query NOT LIKE '%query_log%';
