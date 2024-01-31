-- Tests that sort expression ORDER BY ALL

DROP TABLE IF EXISTS order_by_all;

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
    all UInt64,
)
ENGINE = Memory;

INSERT INTO order_by_all VALUES ('B', 3, 10), ('C', NULL, 40), ('D', 1, 20), ('A', 2, 30);

SELECT '-- no modifiers';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT b, a FROM order_by_all ORDER BY ALL;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT b, a FROM order_by_all ORDER BY ALL;

SELECT '-- with ASC/DESC modifiers';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY ALL ASC;
SELECT a, b FROM order_by_all ORDER BY ALL DESC;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY ALL ASC;
SELECT a, b FROM order_by_all ORDER BY ALL DESC;

SELECT '-- with NULLS FIRST/LAST modifiers';

SET allow_experimental_analyzer = 0;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS LAST;

SET allow_experimental_analyzer = 1;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS LAST;

SELECT '-- "ALL" in ORDER BY is case-insensitive';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT a, b FROM order_by_all ORDER BY all;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT a, b FROM order_by_all ORDER BY all;

SELECT '-- If "all" (case-insensitive) appears in the SELECT clause, throw an error because of ambiguity';

-- columns

SET allow_experimental_analyzer = 0;
SELECT a, b, all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

SET allow_experimental_analyzer = 1;
SELECT a, b, all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

-- column aliases

SET allow_experimental_analyzer = 0;
SELECT a, b AS all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

SET allow_experimental_analyzer = 1;
SELECT a, b AS all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

-- expressions

SET allow_experimental_analyzer = 0;
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

SET allow_experimental_analyzer = 1;
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY ALL;  -- { serverError UNEXPECTED_EXPRESSION }

SELECT '-- If ORDER BY contains "ALL" plus other columns, then "ALL" loses its special meaning';

SET allow_experimental_analyzer = 0;
SELECT a, b, all FROM order_by_all ORDER BY all, a;

SET allow_experimental_analyzer = 1;
SELECT a, b, all FROM order_by_all ORDER BY all, a;

DROP TABLE order_by_all;

SELECT '-- test SELECT * ORDER BY ALL (only works if the SELECT column contains no "all" column)';

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
    c UInt64,
)
ENGINE = Memory;

INSERT INTO order_by_all VALUES ('B', 3, 10), ('C', NULL, 40), ('D', 1, 20), ('A', 2, 30);

SET allow_experimental_analyzer = 0;
SELECT * FROM order_by_all ORDER BY ALL;

SET allow_experimental_analyzer = 1;
SELECT * FROM order_by_all ORDER BY ALL;

DROP TABLE order_by_all;
