-- Tests that sort expression ORDER BY ALL

DROP TABLE IF EXISTS order_by_all;

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32)
)
ENGINE = Memory;

INSERT INTO order_by_all VALUES ('B', 3), ('C', NULL), ('D', 1), ('A', 2);

SELECT '-- no modifiers';

SET enable_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT b, a FROM order_by_all ORDER BY ALL;

SET enable_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT b, a FROM order_by_all ORDER BY ALL;

SELECT '-- with ASC/DESC modifiers';

SET enable_analyzer = 0;
SELECT a, b FROM order_by_all ORDER BY ALL ASC;
SELECT a, b FROM order_by_all ORDER BY ALL DESC;

SET enable_analyzer = 1;
SELECT a, b FROM order_by_all ORDER BY ALL ASC;
SELECT a, b FROM order_by_all ORDER BY ALL DESC;

SELECT '-- with NULLS FIRST/LAST modifiers';

SET enable_analyzer = 0;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS LAST;

SET enable_analyzer = 1;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS FIRST;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS LAST;

SELECT '-- SELECT *';

SET enable_analyzer = 0;
SELECT * FROM order_by_all ORDER BY all;

SET enable_analyzer = 1;
SELECT * FROM order_by_all ORDER BY all;

DROP TABLE order_by_all;

SELECT '-- the trouble starts when "order by all is all" is ambiguous';

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
    all UInt64
)
ENGINE = Memory;

INSERT INTO order_by_all VALUES ('B', 3, 10), ('C', NULL, 40), ('D', 1, 20), ('A', 2, 30);

SELECT '  -- columns';

SET enable_analyzer = 0;
SELECT a, b, all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b, all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;
SELECT a FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;
SELECT * FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT * FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;

SET enable_analyzer = 1;
SELECT a, b, all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b, all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;
SELECT a FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;
-- SELECT * FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION } -- (*) see below
SELECT * FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;
-- SELECT a FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION } -- (*) see below

-- (*) These queries show the expected behavior for analyzer. Unfortunately, it is not implemented that way yet,
-- which is not wrong but a bit unintuitive (some may say a landmine). Keeping the queries for now for reference.

SELECT '  -- column aliases';

SET enable_analyzer = 0;
SELECT a, b AS all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b AS all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;

SET enable_analyzer = 1;
SELECT a, b AS all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b AS all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;

SELECT '  -- expressions';

SET enable_analyzer = 0;
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;

SET enable_analyzer = 1;
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT format('{} {}', a, b) AS all FROM order_by_all ORDER BY all SETTINGS enable_order_by_all = false;

SELECT '  -- ORDER BY ALL loses its special meaning when used in conjunction with other columns';

SET enable_analyzer = 0;
SELECT a, b, all FROM order_by_all ORDER BY all, a;

SET enable_analyzer = 1;
SELECT a, b, all FROM order_by_all ORDER BY all, a;

DROP TABLE order_by_all;
