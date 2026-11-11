-- The composition is resolved only in the analyzer.
SET enable_analyzer = 1;

-- A column or an alias keeps priority over a registered function with the same name,
-- the same way as for a bare function name passed to a higher-order function.
WITH 10 AS negate SELECT arrayMap(negate | toString, [7]); -- { serverError BAD_ARGUMENTS }
WITH 10 AS toString SELECT arrayMap(negate | toString, [7]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(negate | toString, [7]) FROM (SELECT 1 AS negate); -- { serverError BAD_ARGUMENTS }
WITH 10 AS _1 SELECT arrayMap(_1 | toString, [7]); -- { serverError BAD_ARGUMENTS }

-- Without a collision the function names are composed.
SELECT arrayMap(negate | toString, [7]);
SELECT arrayMap(negate | toString, [7]) FROM (SELECT 1 AS x);
-- A lambda bound to the name is still used.
WITH (x -> x + 10) AS negate SELECT arrayMap(negate | toString, [7]);
