-- A parametrised alias is preserved as written, whether or not the parameter has a value.
EXPLAIN TEXT (SELECT 1 AS {name:Identifier}) ONELINE;
EXPLAIN TEXT (WITH 1 AS {name:Identifier} SELECT {name:Identifier}) ONELINE;
SET param_name = 'x';
EXPLAIN TEXT (SELECT 1 AS {name:Identifier}) ONELINE;
SELECT formatQuerySingleLine('SELECT 1 AS {name:Identifier}');
-- Actions refuse a parametrised alias exactly like a plain one.
EXPLAIN TEXT (SELECT 1 LIMIT 3) MODIFY LIMIT (1 AS {name:Identifier}); -- { serverError BAD_ARGUMENTS }
