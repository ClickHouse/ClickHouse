-- Pipe operators: a query can be followed by a chain of |> operators, each wrapping it into a subquery.

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (customer String, amount UInt64, cancelled UInt8) ENGINE = MergeTree ORDER BY customer;
INSERT INTO orders VALUES ('alice', 100, 0), ('alice', 250, 0), ('bob', 50, 1), ('bob', 75, 0), ('charlie', 300, 0);

SELECT '-- A query can start with FROM, and SELECT is optional';
FROM orders SELECT customer, amount ORDER BY customer, amount LIMIT 2;
FROM orders WHERE amount > 200 ORDER BY customer;
FROM orders ORDER BY amount DESC LIMIT 1;

SELECT '-- Table aliases without AS are allowed when SELECT is written explicitly';
FROM orders o SELECT o.customer, o.amount ORDER BY o.customer, o.amount LIMIT 2;
FROM (SELECT customer FROM orders) s SELECT DISTINCT s.customer ORDER BY s.customer;

SELECT '-- Table aliases without AS are also allowed when SELECT is omitted';
FROM orders o WHERE o.amount >= 250 ORDER BY o.amount;
FROM (SELECT customer FROM orders) s WHERE s.customer = 'bob' ORDER BY s.customer;
FROM orders o |> WHERE amount >= 250 |> ORDER BY amount;
-- The word SELECT after the tables starts the explicit SELECT clause, it is not an alias
FROM orders select customer ORDER BY customer LIMIT 1;
FROM orders select |> LIMIT 1; -- { clientError SYNTAX_ERROR }
-- A quoted alias `select` is never the SELECT keyword, and neither is an alias named select inside a subquery
FROM orders `select` WHERE `select`.amount >= 250 ORDER BY `select`.amount;
FROM (SELECT 1 AS select) s WHERE s.`select` = 1 ORDER BY s.`select`;
-- A table named select is not the SELECT keyword either: its own alias without AS keeps working
DROP TABLE IF EXISTS `select`;
CREATE TABLE `select` (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO `select` VALUES (1), (2);
FROM select s WHERE s.id = 1 ORDER BY s.id;
FROM select s |> WHERE id = 2 |> ORDER BY id;
FROM select s INNER JOIN select t ON s.id = t.id WHERE s.id = 2 ORDER BY s.id;
FROM {CLICKHOUSE_DATABASE:Identifier}.select s WHERE s.id = 1 ORDER BY s.id;
FROM select s SELECT s.id ORDER BY s.id;
DROP TABLE `select`;
-- An alias select in the middle of the FROM clause cannot be the SELECT keyword (only the alias that
-- ends the FROM clause can), so a joined table gets to keep it even when SELECT is omitted
FROM numbers(2) AS l INNER JOIN (SELECT number AS id FROM numbers(2)) select ON l.number = select.id WHERE select.id = 1;
FROM numbers(2) AS l INNER JOIN (SELECT number AS id FROM numbers(2)) select ON l.number = select.id |> WHERE id = 0 |> SELECT id;

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

SELECT '-- WITH: query-scoped aliases and CTEs stay visible in the following operators';
WITH 100 AS threshold FROM orders |> WHERE amount >= threshold |> AGGREGATE count() AS c;
WITH 100 AS threshold FROM orders |> SELECT customer, amount >= threshold AS is_big |> ORDER BY customer, is_big;
WITH big AS (FROM orders |> WHERE amount >= 250) FROM big |> SELECT customer |> ORDER BY customer;
WITH src AS (SELECT 'alice' AS customer) FROM orders |> AS o |> JOIN src AS s USING (customer) |> AGGREGATE count() AS c;

SELECT '-- AS and JOIN: table aliases are visible inside the same operator, following operators see the combined columns';
FROM orders |> AGGREGATE sum(amount) AS total GROUP BY customer |> AS agg |> JOIN (FROM orders |> WHERE amount = 300 |> SELECT customer AS c) AS big ON agg.customer = big.c |> SELECT customer, total;

SELECT '-- ARRAY JOIN';
FROM orders |> WHERE customer = 'alice' |> EXTEND [1, 2] AS arr |> ARRAY JOIN arr |> SELECT customer, amount, arr |> ORDER BY amount, arr;
-- A comma-separated expression list in ARRAY JOIN keeps working, and a comma join right after an
-- ARRAY JOIN is rejected, same as in the FROM clause of an ordinary query
FROM orders |> WHERE customer = 'charlie' |> EXTEND [1, 2] AS arr, [3, 4] AS brr |> ARRAY JOIN arr, brr |> SELECT customer, arr, brr |> ORDER BY arr, brr;
FROM orders |> EXTEND [1] AS arr |> ARRAY JOIN arr, (b, view(SELECT 1 AS A), x); -- { clientError SYNTAX_ERROR }
-- The comma spelling of a cross join is supported, with the input of the operator as the left side
FROM orders |> WHERE customer = 'charlie' |> AS o |> , (SELECT 1 AS one) AS x |> SELECT customer, one;
FROM orders |> WHERE customer = 'charlie' |> AS o |> , (SELECT 1 AS one) AS x, (SELECT 2 AS two) AS y |> AGGREGATE count();

SELECT '-- Set operations';
FROM orders |> WHERE customer = 'alice' |> SELECT customer |> UNION ALL (FROM orders |> WHERE customer = 'bob' |> SELECT customer) |> DISTINCT |> ORDER BY customer;
FROM orders |> SELECT customer |> INTERSECT DISTINCT (SELECT 'bob') |> ORDER BY customer;
FROM orders |> SELECT DISTINCT customer |> EXCEPT (SELECT 'bob') |> ORDER BY customer;
SELECT 1 AS x UNION ALL SELECT 2 |> AGGREGATE count() AS c;

SELECT '-- A pipe operator continues after a set operation only if the last operand is parenthesized';
SELECT count() FROM (FROM orders |> SELECT customer |> UNION ALL SELECT 'zed');
FROM orders |> SELECT customer |> UNION ALL (SELECT 'bob') |> WHERE customer = 'bob' |> AGGREGATE count() AS c;

SELECT '-- Pipe operators after a regular SELECT query';
SELECT number AS n FROM numbers(10) |> WHERE n % 3 = 0 |> AGGREGATE count() AS c;

SELECT '-- Pipe operators in a subquery';
SELECT count() FROM (FROM orders |> WHERE cancelled = 0);

SELECT '-- INSERT from a pipe query';
DROP TABLE IF EXISTS big_orders;
CREATE TABLE big_orders (customer String, amount UInt64) ENGINE = MergeTree ORDER BY customer;
INSERT INTO big_orders FROM orders |> WHERE amount >= 250 |> SELECT customer, amount;
FROM big_orders ORDER BY customer;

SELECT '-- An INSERT-scoped WITH stays visible in the pipe operators';
DROP TABLE IF EXISTS filtered_orders;
CREATE TABLE filtered_orders (customer String, amount UInt64) ENGINE = MergeTree ORDER BY customer;
WITH 100 AS lo, 300 AS hi INSERT INTO filtered_orders FROM orders |> WHERE amount >= lo |> WHERE amount < hi |> SELECT customer, amount;
FROM filtered_orders ORDER BY customer, amount;

SELECT '-- Without enable_global_with_statement, an INSERT-scoped WITH does not reach the pipe stages, same as a hand-written nested subquery';
SET enable_global_with_statement = 0;
WITH 100 AS lo INSERT INTO filtered_orders FROM orders |> WHERE amount >= lo |> SELECT customer, amount; -- { serverError UNKNOWN_IDENTIFIER }
WITH 100 AS lo INSERT INTO filtered_orders SELECT customer, amount FROM (SELECT * FROM orders WHERE amount >= lo); -- { serverError UNKNOWN_IDENTIFIER }
SET enable_global_with_statement = 1;

SELECT '-- The ORDER BY operator supports the full clause syntax: ORDER BY ALL, WITH FILL, INTERPOLATE';
FROM orders |> SELECT customer, amount |> ORDER BY ALL;
FROM orders |> SELECT customer |> DISTINCT |> ORDER BY ALL DESC;
SELECT 1 AS x, 10 AS y |> ORDER BY x WITH FILL FROM 1 TO 4;
SELECT 1 AS x, 10 AS y |> ORDER BY x WITH FILL FROM 1 TO 4 INTERPOLATE (y AS y + 1);

SELECT '-- SAMPLE with OFFSET requires an explicit SELECT in the FROM-first form';
DROP TABLE IF EXISTS sampled;
CREATE TABLE sampled (id UInt64) ENGINE = MergeTree ORDER BY intHash32(id) SAMPLE BY intHash32(id);
INSERT INTO sampled SELECT number FROM numbers(10);
FROM sampled SAMPLE 1 SELECT * ORDER BY id OFFSET 8;
FROM sampled SAMPLE 1 OFFSET 0 SELECT * ORDER BY id LIMIT 2;
FROM sampled SAMPLE 1 OFFSET 0; -- { clientError SYNTAX_ERROR }
FROM sampled SAMPLE 1 OFFSET 0 |> LIMIT 1; -- { clientError SYNTAX_ERROR }
-- When the query continues with a clause that a query-level OFFSET cannot precede, there is no
-- ambiguity and SELECT stays optional
FROM sampled SAMPLE 1 OFFSET 0 WHERE id > 8;
FROM sampled SAMPLE 1 OFFSET 0 ORDER BY id LIMIT 2;
FROM sampled SAMPLE 1 OFFSET 0 GROUP BY id ORDER BY id LIMIT 1;
-- The sample offset is unambiguous when it does not end the tables of the query either
FROM sampled SAMPLE 1 OFFSET 0 JOIN sampled AS dim USING (id) ORDER BY id LIMIT 1;
FROM sampled SAMPLE 1 OFFSET 0 JOIN sampled AS dim USING (id) |> ORDER BY id |> LIMIT 1;
DROP TABLE sampled;

SELECT '-- A trailing comma is allowed in the select-list-like operators, as in an ordinary SELECT clause';
FROM orders |> SELECT customer, amount, |> ORDER BY customer, amount |> LIMIT 2;
FROM orders |> WHERE amount >= 250 |> SELECT customer, amount, |> ORDER BY customer;
FROM orders |> WHERE amount >= 250 |> EXTEND amount * 2, |> ORDER BY customer;
FROM orders |> AGGREGATE count(), sum(amount), |> LIMIT 1;
FROM orders |> SELECT DISTINCT customer, |> ORDER BY customer;
-- A trailing comma is also allowed at the very end of the query
FROM orders |> WHERE customer = 'charlie' |> SELECT customer, amount,;
-- A trailing comma is not accepted in front of a clause keyword, exactly as in an ordinary SELECT clause
FROM orders |> AGGREGATE count() AS c, GROUP BY customer; -- { clientError SYNTAX_ERROR }
FROM orders |> SELECT customer, , amount; -- { clientError SYNTAX_ERROR }

SELECT '-- A pipe operator can end with a SETTINGS clause, attached to the generated wrapper query';
FROM orders |> WHERE amount >= 250 |> ORDER BY customer SETTINGS max_threads = 1;
-- The settings reach execution, same as in the equivalent nested form
FROM orders |> LIMIT 1 SETTINGS max_rows_to_read = 2; -- { serverError TOO_MANY_ROWS }
-- A SETTINGS clause in the middle of a chain stays on its stage, which becomes a subquery
FROM orders |> ORDER BY amount DESC |> LIMIT 2 SETTINGS max_threads = 1 |> WHERE customer = 'charlie';
-- A trailing SETTINGS works in the contexts that have no separate pass for query settings
SELECT count() FROM (FROM orders |> WHERE cancelled = 0 SETTINGS max_threads = 1);
SELECT count() FROM view(FROM orders |> LIMIT 2 SETTINGS max_threads = 1);
DROP TABLE IF EXISTS pipe_settings_view;
CREATE VIEW pipe_settings_view AS FROM orders |> WHERE amount > 200 |> SELECT customer SETTINGS max_threads = 1;
SELECT * FROM pipe_settings_view ORDER BY customer;
DROP TABLE pipe_settings_view;
-- SETTINGS goes before INTERPOLATE in the generated wrapper, as in an ordinary SELECT query
SELECT 1 AS x, 10 AS y |> ORDER BY x WITH FILL FROM 1 TO 4 INTERPOLATE (y AS y + 1) SETTINGS max_threads = 1;

SET enable_analyzer = 1;

SELECT '-- The resulting AST is the same as with nested subqueries';
EXPLAIN SYNTAX oneline = 1 FROM orders |> WHERE cancelled = 0 |> AGGREGATE sum(amount) AS total GROUP BY customer |> ORDER BY total DESC |> LIMIT 3;
EXPLAIN SYNTAX oneline = 1 FROM orders |> SET amount = amount + 1 |> DROP cancelled |> AS t |> LEFT JOIN big_orders AS b USING (customer);
EXPLAIN SYNTAX oneline = 1 FROM orders |> ORDER BY amount DESC |> LIMIT 2 OFFSET 1;
EXPLAIN SYNTAX oneline = 1 WITH 100 AS threshold FROM orders |> WHERE amount >= threshold |> AGGREGATE count() AS c;

SELECT '-- WITH RECURSIVE stays visible across pipe operators (requires the analyzer)';
WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) FROM r |> AGGREGATE sum(n) AS s;

SELECT '-- A column alias list can follow the tables in the FROM-first form, before the SELECT clause (requires the analyzer)';
FROM (SELECT customer, amount FROM orders) AS o (name, total) SELECT name, total |> WHERE total >= 250 |> ORDER BY name, total;
FROM (SELECT customer, amount FROM orders) AS o (name, total) |> WHERE total >= 250 |> SELECT name |> ORDER BY name;

SELECT '-- A CTE column alias list stays intact across pipe operators (requires the analyzer)';
WITH t(a) AS (SELECT 1 AS x) FROM t |> SELECT a |> LIMIT 1;

SELECT '-- Ordinary settings of the query before the first pipe operator keep working (requires the analyzer)';
SELECT count() FROM numbers(10) SETTINGS max_rows_to_read = 5 |> LIMIT 1; -- { serverError TOO_MANY_ROWS }
SELECT number FROM numbers(2) SETTINGS max_block_size = 1 |> ORDER BY number;

SELECT '-- An analyzer setting before the first pipe operator becomes a subquery override, exactly as in the hand-written nested form (requires the analyzer)';

SELECT '-- Errors';
FROM orders |> FOO; -- { clientError SYNTAX_ERROR }
FROM orders |>; -- { clientError SYNTAX_ERROR }
SELECT 1 |> WHERE; -- { clientError SYNTAX_ERROR }
FROM orders |> LIMIT 1 UNION ALL SELECT 'x', 0, 0; -- { clientError SYNTAX_ERROR }
FROM orders |> SELECT customer |> UNION ALL SELECT 'zed' |> WHERE customer = 'bob'; -- { clientError SYNTAX_ERROR }

DROP TABLE filtered_orders;
DROP TABLE big_orders;
DROP TABLE orders;
