-- Tests that sort expression ORDER BY *

DROP TABLE IF EXISTS order_by_all;

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
)
ENGINE = Memory;

INSERT INTO order_by_all VALUES ('B', 3), ('C', NULL), ('D', 1), ('A', 2);

SELECT '-- no modifiers';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY *;
SELECT b, a FROM order_by_all ORDER BY *;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY *;
SELECT b, a FROM order_by_all ORDER BY *;

SELECT '-- with ASC/DESC modifiers';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY * ASC;
SELECT a, b FROM order_by_all ORDER BY * DESC;

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY * ASC;
SELECT a, b FROM order_by_all ORDER BY * DESC;

SELECT '-- with NULLS FIRST/LAST modifiers';

SET allow_experimental_analyzer = 0;
SELECT b, a FROM order_by_all ORDER BY * NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY * NULLS LAST;

SET allow_experimental_analyzer = 1;
SELECT b, a FROM order_by_all ORDER BY * NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY * NULLS LAST;

SELECT '-- Special case: all columns in SELECT clause, ORDER BY *';
SELECT * FROM order_by_all ORDER BY * NULLS LAST;

SELECT '-- "*" must appear stand-alone in ORDER BY';

SET allow_experimental_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY *, a; -- { serverError UNKNOWN_IDENTIFIER }

SET allow_experimental_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY *, a; -- { serverError UNSUPPORTED_METHOD }
