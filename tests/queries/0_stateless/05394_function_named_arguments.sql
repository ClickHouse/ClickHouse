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
