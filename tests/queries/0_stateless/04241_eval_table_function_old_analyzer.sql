SET allow_experimental_eval_table_function = 1;
SET enable_analyzer = 0;

SELECT * FROM eval(SELECT 'SELECT 1 AS x');
SELECT * FROM eval(SELECT 'SELECT 2 AS x UNION ALL SELECT 3 AS x') ORDER BY x;
