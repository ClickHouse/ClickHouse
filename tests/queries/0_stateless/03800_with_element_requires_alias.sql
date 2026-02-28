-- Expressions in WITH clause must have an alias.
-- Tests are organized by list size (1–3 elements) and which elements are missing aliases.

-- 1 element, no alias
WITH 1 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2) SELECT 1; -- { clientError SYNTAX_ERROR }
WITH 'hello' || ' world' SELECT 1; -- { clientError SYNTAX_ERROR }

-- 2 elements: first missing alias
WITH 1, 2 AS b SELECT b; -- { clientError SYNTAX_ERROR }
WITH (1 + 2) * 3, 'x' AS b SELECT b; -- { clientError SYNTAX_ERROR }
WITH NULL, 'fallback' AS b SELECT b; -- { clientError SYNTAX_ERROR }

-- 2 elements: second missing alias
WITH 1 AS a, 2 SELECT a; -- { clientError SYNTAX_ERROR }
WITH 'hello' AS a, pow(2, 2) SELECT a; -- { clientError SYNTAX_ERROR }
WITH [1, 2, 3] AS a, 1 > 0 SELECT a; -- { clientError SYNTAX_ERROR }

-- 2 elements: both missing aliases
WITH 1, 2 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2), today() SELECT 1; -- { clientError SYNTAX_ERROR }
WITH CAST(1 AS Float64), 'hello' SELECT 1; -- { clientError SYNTAX_ERROR }

-- 3 elements: only first missing alias
WITH 1, 2 AS b, 3 AS c SELECT b, c; -- { clientError SYNTAX_ERROR }
WITH if(1, 2, 3), 'x' AS b, NULL AS c SELECT b, c; -- { clientError SYNTAX_ERROR }

-- 3 elements: only second missing alias
WITH 1 AS a, 2, 3 AS c SELECT a, c; -- { clientError SYNTAX_ERROR }
WITH 'hello' AS a, pow(2, 2), [1, 2] AS c SELECT a, c; -- { clientError SYNTAX_ERROR }

-- 3 elements: only third missing alias
WITH 1 AS a, 2 AS b, 3 SELECT a, b; -- { clientError SYNTAX_ERROR }
WITH NULL AS a, 'x' AS b, (1 + 2) * 3 SELECT a, b; -- { clientError SYNTAX_ERROR }

-- 3 elements: first and second missing aliases
WITH 1, 2, 3 AS c SELECT c; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2), if(1, 2, 3), 'z' AS c SELECT c; -- { clientError SYNTAX_ERROR }

-- 3 elements: first and third missing aliases
WITH 1, 2 AS b, 3 SELECT b; -- { clientError SYNTAX_ERROR }
WITH NULL, 'x' AS b, pow(2, 2) SELECT b; -- { clientError SYNTAX_ERROR }

-- 3 elements: second and third missing aliases
WITH 1 AS a, 2, 3 SELECT a; -- { clientError SYNTAX_ERROR }
WITH [1, 2] AS a, 'hello', today() SELECT a; -- { clientError SYNTAX_ERROR }

-- 3 elements: all missing aliases
WITH 1, 2, 3 SELECT 1; -- { clientError SYNTAX_ERROR }
WITH pow(2, 2), today(), NULL SELECT 1; -- { clientError SYNTAX_ERROR }