-- Pipe operators: a query can be followed by a chain of |> operators, each wrapping it into a subquery.

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (customer String, amount UInt64, cancelled UInt8) ENGINE = MergeTree ORDER BY customer;
INSERT INTO orders VALUES ('alice', 100, 0), ('alice', 250, 0), ('bob', 50, 1), ('bob', 75, 0), ('charlie', 300, 0);

SELECT '-- A query can start with FROM, and SELECT is optional';
FROM orders SELECT customer, amount ORDER BY customer, amount LIMIT 2;
FROM orders WHERE amount > 200 ORDER BY customer;
FROM orders ORDER BY amount DESC LIMIT 1;

SELECT '-- WHERE';
FROM orders |> WHERE amount >= 100 |> WHERE cancelled = 0 |> ORDER BY amount;

SELECT '-- SELECT';
FROM orders |> WHERE customer = 'alice' |> SELECT customer, amount * 2 AS doubled |> ORDER BY doubled;

SELECT '-- SELECT DISTINCT and DISTINCT';
FROM orders |> SELECT DISTINCT cancelled |> ORDER BY cancelled;
FROM orders |> SELECT cancelled |> DISTINCT |> ORDER BY cancelled;

SELECT '-- EXTEND';
FROM orders |> WHERE customer = 'bob' |> EXTEND amount * 10 AS big, length(customer) AS len |> ORDER BY amount;

SELECT '-- SET';
FROM orders |> WHERE customer = 'bob' |> SET amount = amount + 1000 |> ORDER BY amount;

SELECT '-- DROP';
FROM orders |> WHERE customer = 'charlie' |> DROP cancelled;

SELECT '-- AGGREGATE with GROUP BY: grouping columns come first';
FROM orders |> WHERE cancelled = 0 |> AGGREGATE count() AS c, sum(amount) AS total GROUP BY customer |> ORDER BY customer;

SELECT '-- AGGREGATE with an alias in GROUP BY';
FROM orders |> AGGREGATE sum(amount) AS total GROUP BY cancelled AS status |> ORDER BY status;

SELECT '-- AGGREGATE of the whole table';
FROM orders |> AGGREGATE count() AS c, max(amount) AS m;

SELECT '-- WHERE after AGGREGATE works as HAVING';
FROM orders |> AGGREGATE sum(amount) AS total GROUP BY customer |> WHERE total > 100 |> ORDER BY customer;

SELECT '-- ORDER BY, LIMIT, OFFSET';
FROM orders |> ORDER BY amount DESC |> LIMIT 2;
FROM orders |> ORDER BY amount DESC |> LIMIT 2 OFFSET 1;
FROM orders |> ORDER BY amount DESC |> LIMIT 100 |> OFFSET 3;

SELECT '-- AS and JOIN';
FROM orders |> AGGREGATE sum(amount) AS total GROUP BY customer |> AS agg |> JOIN orders AS o ON agg.customer = o.customer |> WHERE o.amount = 300 |> SELECT agg.customer, total;

SELECT '-- ARRAY JOIN';
FROM orders |> WHERE customer = 'alice' |> EXTEND [1, 2] AS arr |> ARRAY JOIN arr |> SELECT customer, amount, arr |> ORDER BY amount, arr;

SELECT '-- Set operations';
FROM orders |> WHERE customer = 'alice' |> SELECT customer |> UNION ALL (FROM orders |> WHERE customer = 'bob' |> SELECT customer) |> DISTINCT |> ORDER BY customer;
FROM orders |> SELECT customer |> INTERSECT DISTINCT (SELECT 'bob') |> ORDER BY customer;
FROM orders |> SELECT DISTINCT customer |> EXCEPT (SELECT 'bob') |> ORDER BY customer;
SELECT 1 AS x UNION ALL SELECT 2 |> AGGREGATE count() AS c;

SELECT '-- Pipe operators after a regular SELECT query';
SELECT number AS n FROM numbers(10) |> WHERE n % 3 = 0 |> AGGREGATE count() AS c;

SELECT '-- Pipe operators in a subquery';
SELECT count() FROM (FROM orders |> WHERE cancelled = 0);

SELECT '-- INSERT from a pipe query';
DROP TABLE IF EXISTS big_orders;
CREATE TABLE big_orders (customer String, amount UInt64) ENGINE = MergeTree ORDER BY customer;
INSERT INTO big_orders FROM orders |> WHERE amount >= 250 |> SELECT customer, amount;
FROM big_orders ORDER BY customer;

SELECT '-- The resulting AST is the same as with nested subqueries';
EXPLAIN SYNTAX oneline = 1 FROM orders |> WHERE cancelled = 0 |> AGGREGATE sum(amount) AS total GROUP BY customer |> ORDER BY total DESC |> LIMIT 3;
EXPLAIN SYNTAX oneline = 1 FROM orders |> SET amount = amount + 1 |> DROP cancelled |> AS t |> LEFT JOIN big_orders AS b USING (customer);

SELECT '-- Errors';
FROM orders |> FOO; -- { clientError SYNTAX_ERROR }
FROM orders |>; -- { clientError SYNTAX_ERROR }
SELECT 1 |> WHERE; -- { clientError SYNTAX_ERROR }
FROM orders |> LIMIT 1 UNION ALL SELECT 'x', 0, 0; -- { clientError SYNTAX_ERROR }

DROP TABLE big_orders;
DROP TABLE orders;
