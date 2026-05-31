SELECT * FROM eval('SELECT 1'); -- { serverError BAD_ARGUMENTS }

SET allow_experimental_eval_table_function = 1;

SELECT count() FROM system.table_functions WHERE name = 'eval';

SELECT * FROM eval('SEL' || 'ECT 1 AS x');

SET param_q = 'SELECT 2 AS y';
SELECT * FROM eval({q:String});

SELECT * FROM eval(SELECT 'SELECT 3 AS z');
SELECT x + 1 FROM eval('SELECT 4 AS x');

SELECT * FROM eval(SELECT toLowCardinality('SELECT 5 AS lc'));
SELECT * FROM eval(SELECT CAST('SELECT 6 AS n', 'Nullable(String)'));
SELECT * FROM eval(SELECT toLowCardinality(CAST('SELECT 7 AS lcn', 'Nullable(String)')));

SELECT count() > 0 FROM (EXPLAIN PLAN SELECT * FROM eval('SELECT 8 AS explain_value'));
WITH 'SELECT 9 AS with_alias_value' AS q SELECT * FROM eval(q);

SELECT n FROM eval(SELECT 'SELECT 2 AS n UNION ALL SELECT 1 AS n') ORDER BY n;
SELECT * FROM eval(SELECT 'SELECT 1 AS n INTERSECT SELECT 1 AS n');
SELECT * FROM eval(SELECT 'SELECT 1 AS n EXCEPT SELECT 2 AS n');

SELECT * FROM eval(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM eval(SELECT 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM eval(CAST(NULL, 'Nullable(String)')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(toString(rand())); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(SELECT 'SELECT 1' WHERE 0); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(SELECT arrayJoin(['SELECT 1', 'SELECT 2'])); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval(SELECT 'SELECT 1', 'SELECT 2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval('SHOW TABLES'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval('SELECT 1; SELECT 2'); -- { serverError SYNTAX_ERROR }
SELECT * FROM eval('SELECT * FROM eval(''SELECT 1'')'); -- { serverError BAD_ARGUMENTS }
