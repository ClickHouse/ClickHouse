-- Tags: no-parallel
-- no-parallel: Creates the global SQL UDF `eval`, which is not isolated by the test database.

DROP FUNCTION IF EXISTS eval;

CREATE FUNCTION eval AS x -> x + 1;

SELECT eval(1) FORMAT TSVWithNames;

SET allow_experimental_eval_table_function = 1;
SELECT * FROM eval('SELECT eval(1)');
SELECT * FROM eval('SELECT count() FROM numbers(eval(1))');

SET enable_analyzer = 0;
SELECT eval(1) FORMAT TSVWithNames;
SET enable_analyzer = 1;

DROP FUNCTION eval;
