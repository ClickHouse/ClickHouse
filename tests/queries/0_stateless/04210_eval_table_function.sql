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
WITH 'SEL' AS a, 'ECT 10 AS with_concat_value' AS b SELECT * FROM eval(a || b);
SELECT count() FROM eval('SELECT number FROM numbers(3) SETTINGS limit = 1');

SET enable_analyzer = 0;
WITH 'SELECT 1' AS x SELECT * FROM eval(arrayElement(arrayMap(x -> x, ['SELECT 11 AS lambda_value']), 1));
SET enable_analyzer = 1;

SET union_default_mode = 'ALL';
SELECT count() FROM eval('SELECT 1 AS x UNION SELECT 1 SETTINGS union_default_mode = ''DISTINCT''');
SET intersect_default_mode = 'DISTINCT';
SELECT count() FROM eval('SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1) INTERSECT SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1) SETTINGS intersect_default_mode = ''ALL''');
SET except_default_mode = 'DISTINCT';
SELECT count() FROM eval('SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1) EXCEPT SELECT * FROM (SELECT 1 AS x) SETTINGS except_default_mode = ''ALL''');

SELECT n FROM eval(SELECT 'SELECT 2 AS n UNION ALL SELECT 1 AS n') ORDER BY n;
SELECT * FROM eval(SELECT 'SELECT 1 AS n INTERSECT SELECT 1 AS n');
SELECT * FROM eval(SELECT 'SELECT 1 AS n EXCEPT SELECT 2 AS n');
SELECT * FROM eval(SELECT arrayJoin(['SELECT 1', 'SELECT 2']) SETTINGS limit = 1);
SELECT * FROM eval(SELECT arrayJoin(['SELECT 1', 'SELECT 2']) SETTINGS limit = 1, offset = 1);

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
SELECT * FROM eval('SELECT * FROM remote(''remote'', eval(''SELECT 1''))'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval('SELECT * FROM loop(eval(''SELECT 1'')) LIMIT 1'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM eval('SELECT * FROM viewIfPermitted(SELECT 1 AS x ELSE eval(''SELECT 1 AS x''))'); -- { serverError BAD_ARGUMENTS }
