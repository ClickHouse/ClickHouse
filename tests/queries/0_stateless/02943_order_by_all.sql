DROP TABLE IF EXISTS order_by_all;

CREATE TABLE order_by_all
(
    a String,
    b Nullable(Int32),
    all int,
)
    engine = Memory;

INSERT INTO order_by_all VALUES ('B', 3, 2), ('C', NULL, 3), ('B', 1, 1), ('A', 3, 2);

SELECT a, b FROM order_by_all ORDER BY ALL;
SELECT a, b, all FROM order_by_all ORDER BY all;  -- { serverError UNEXPECTED_EXPRESSION }
SELECT a, b, all FROM order_by_all ORDER BY all, a;
SELECT a, b, all FROM order_by_all ORDER BY all settings enable_order_by_all = false;
SELECT a, b FROM order_by_all ORDER BY ALL DESC;
SELECT b, a FROM order_by_all ORDER BY ALL NULLS FIRST;

DROP TABLE IF EXISTS order_by_all;

