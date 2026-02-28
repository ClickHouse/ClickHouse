-- Expressions in WITH clause must have an alias

WITH 1 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 'hello' SELECT 1; -- { clientError SYNTAX_ERROR }
WITH NULL SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 1 + 2 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH (1 + 2) * 3 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 1 > 0 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 'hello' || ' world' SELECT 1; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2) SELECT 1; -- { clientError SYNTAX_ERROR }
WITH CAST(1 AS Float64) SELECT 1; -- { clientError SYNTAX_ERROR }
WITH CASE WHEN 1 = 1 THEN 2 ELSE 3 END SELECT 1; -- { clientError SYNTAX_ERROR }
WITH [1, 2, 3] SELECT 1; -- { clientError SYNTAX_ERROR }
WITH (1, 2, 3) SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 3, 1 AS x SELECT x; -- { clientError SYNTAX_ERROR }
WITH 1 AS x, 2 SELECT x; -- { clientError SYNTAX_ERROR }
WITH 1 AS x, 2 AS y, 3 SELECT x, y; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2) AS four, if(1, 2, 3) SELECT four; -- { clientError SYNTAX_ERROR }
