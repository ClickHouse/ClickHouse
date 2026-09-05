-- With joined_subquery_requires_alias = 1 (the default) the analyzer requires an alias for a subquery or
-- table function in a JOIN only when the alias is actually needed to qualify a name: an identifier that
-- resolves to different columns of several joined table expressions, or a matcher that produces several
-- columns with the same name, where one of these columns belongs to the unaliased subquery or table function.
-- Unambiguous queries are allowed without the alias.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS with_number;
DROP TABLE IF EXISTS mt;

CREATE TABLE item (item_id Int32, brand Int32) ENGINE = Memory;
CREATE TABLE sales (s_item Int32, s_brand Int32) ENGINE = Memory;
CREATE TABLE with_number (number Int32) ENGINE = Memory;
CREATE TABLE mt (id UInt8) ENGINE = MergeTree ORDER BY id;

INSERT INTO item VALUES (1, 100), (2, 200);
INSERT INTO sales VALUES (10, 100), (20, 999);
INSERT INTO with_number VALUES (1), (2);
INSERT INTO mt VALUES (1), (2);

SELECT '-- No ambiguity: the alias is not needed';
SELECT item_id FROM item, (SELECT s_brand AS xbrand FROM sales) WHERE brand = xbrand ORDER BY item_id;
SELECT item_id FROM item JOIN (SELECT s_item, s_brand FROM sales) ON brand = s_brand ORDER BY item_id;
SELECT item_id FROM item, numbers(3) WHERE item_id = number ORDER BY item_id;
SELECT item_id FROM item, (SELECT s_brand AS xb FROM sales INTERSECT SELECT s_brand FROM sales) WHERE brand = xb ORDER BY item_id;
SELECT count() FROM with_number, numbers(3);
SELECT x FROM (SELECT 1 AS x) PASTE JOIN (SELECT 2 AS y);

SELECT '-- Ambiguous identifier from an unaliased subquery or table function: the alias is required';
SELECT brand FROM item, (SELECT s_brand AS brand FROM sales); -- { serverError ALIAS_REQUIRED }
SELECT 1 FROM item, (SELECT s_brand AS brand FROM sales) WHERE brand = 100; -- { serverError ALIAS_REQUIRED }
SELECT item_id FROM item JOIN (SELECT s_item AS item_id, s_brand AS brand FROM sales) ON item.brand = brand; -- { serverError ALIAS_REQUIRED }
SELECT number FROM with_number, numbers(3); -- { serverError ALIAS_REQUIRED }
SELECT x FROM (SELECT 1 AS x) AS a, (SELECT 2 AS y), (SELECT 3 AS x); -- { serverError ALIAS_REQUIRED }
SELECT x FROM (SELECT 1 AS x) AS a JOIN (SELECT 2 AS y) ON true JOIN (SELECT 3 AS x) ON true; -- { serverError ALIAS_REQUIRED }
SELECT x FROM (SELECT 1 AS x) JOIN (SELECT 2 AS x) AS b ON true; -- { serverError ALIAS_REQUIRED }

SELECT '-- Two structurally identical table expressions resolve the identifier by the alias, so it is ambiguous as well';
SELECT number FROM numbers(2) AS x, numbers(2) WHERE x.number = 0; -- { serverError ALIAS_REQUIRED }
SELECT number FROM numbers(2), numbers(2) AS y WHERE y.number = 0; -- { serverError ALIAS_REQUIRED }
SELECT number FROM numbers(2), numbers(2); -- { serverError ALIAS_REQUIRED }
SELECT a FROM (SELECT number AS a FROM numbers(2)) AS x LEFT JOIN (SELECT number AS a FROM numbers(2)) ON x.a = 0; -- { serverError ALIAS_REQUIRED }
SELECT a FROM (SELECT number AS a FROM numbers(2)) LEFT JOIN (SELECT number AS a FROM numbers(2)) AS y ON y.a = 0; -- { serverError ALIAS_REQUIRED }
SELECT number FROM numbers(2) AS x, numbers(2) AS y WHERE x.number = 0 ORDER BY number;

SELECT '-- Virtual columns of a sibling table participate in the ambiguity';
SELECT _part FROM mt, (SELECT '' AS _part); -- { serverError ALIAS_REQUIRED }
SELECT id FROM mt, (SELECT '' AS not_a_column) ORDER BY id;

SELECT '-- Subcolumns are ambiguous as well';
SELECT t.a FROM (SELECT tuple(1)::Tuple(a UInt8) AS t) AS lhs, (SELECT tuple(2)::Tuple(a UInt8) AS t); -- { serverError ALIAS_REQUIRED }
SELECT t.a FROM (SELECT tuple(1)::Tuple(a UInt8) AS t) AS lhs, (SELECT tuple(2)::Tuple(b UInt8) AS u);

SELECT '-- A subquery wrapped into ARRAY JOIN still needs the alias for its ambiguous column';
SELECT x FROM (SELECT [1] AS arr, 2 AS x) ARRAY JOIN arr INNER JOIN (SELECT 0 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }
SELECT y, x, arr FROM (SELECT [1] AS arr, 2 AS x) ARRAY JOIN arr INNER JOIN (SELECT 0 AS y) AS rhs ON true;

SELECT '-- The named side can still be qualified, and USING columns are not ambiguous';
SELECT item.brand FROM item, (SELECT s_brand AS brand FROM sales) ORDER BY item.brand;
SELECT brand FROM item JOIN (SELECT s_brand AS brand FROM sales) USING (brand) ORDER BY brand;

SELECT '-- A CTE has a name to qualify with, so it is not an unaliased subquery';
WITH cte AS (SELECT 1 AS x) SELECT cte.x, a.x FROM (SELECT 2 AS x) AS a, cte;

SELECT '-- An expression alias shadows the joined column by the usual rules, this is not an ambiguity';
WITH 1 AS x SELECT x FROM numbers(1), (SELECT 2 AS x);

SELECT '-- Matcher: several columns with the same name require the alias';
SELECT * FROM item, (SELECT s_brand AS brand FROM sales); -- { serverError ALIAS_REQUIRED }
SELECT * FROM (SELECT 1 AS A, 2 AS B) X ALL LEFT JOIN (SELECT 3 AS A, 2 AS B) USING (B); -- { serverError ALIAS_REQUIRED }
SELECT * FROM item, (SELECT 1 AS other) ORDER BY item_id FORMAT TSVWithNames;
SELECT * FROM item, (SELECT 100 AS brand) AS sub ORDER BY item_id FORMAT TSVWithNames;
SELECT * FROM (SELECT 100 AS brand), item ORDER BY item_id FORMAT TSVWithNames;
SELECT * EXCEPT (brand) FROM item, (SELECT 100 AS brand) ORDER BY item_id FORMAT TSVWithNames;
SELECT COLUMNS('brand') FROM item, (SELECT 100 AS brand); -- { serverError ALIAS_REQUIRED }

SELECT '-- Matcher: a column transformer does not hide the source of the matched column';
SELECT * APPLY (x -> 1 + x) FROM (SELECT 1 AS a), (SELECT 2 AS a); -- { serverError ALIAS_REQUIRED }
SELECT * APPLY (x -> concat('p', toString(x))) FROM (SELECT 1 AS a), (SELECT 2 AS a); -- { serverError ALIAS_REQUIRED }
SELECT * APPLY (x -> 1 + x) FROM (SELECT 1 AS a) AS l, (SELECT 2 AS a); -- { serverError ALIAS_REQUIRED }
SELECT * REPLACE (0 AS brand) FROM item, (SELECT 100 AS brand); -- { serverError ALIAS_REQUIRED }
SELECT * APPLY (x -> 1 + x) FROM (SELECT 1 AS a) AS l, (SELECT 2 AS a) AS r FORMAT TSVWithNames;
SELECT * APPLY (x -> 1 + x) FROM (SELECT 1 AS a), (SELECT 2 AS b) FORMAT TSVWithNames;
SELECT * APPLY (x -> 1 + x) FROM item, (SELECT 1 AS other) ORDER BY item_id FORMAT TSVWithNames;

SELECT '-- Equally named columns of a single table expression do not need a qualification';
SELECT * FROM (SELECT x, x FROM (SELECT 1 AS x));
SELECT * FROM (SELECT x, x FROM (SELECT 1 AS x)), (SELECT 3 AS y);

SELECT '-- PASTE JOIN allows equally named columns, but only for the columns it combines itself';
SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b) PASTE JOIN (SELECT 3 AS a); -- { serverError AMBIGUOUS_COLUMN_NAME }
SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b), (SELECT 3 AS c) FORMAT TSVWithNames;
SELECT * FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b), (SELECT 3 AS a); -- { serverError ALIAS_REQUIRED }
SELECT COLUMNS('a') FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b), (SELECT 3 AS a); -- { serverError ALIAS_REQUIRED }
SELECT a FROM (SELECT 1 AS a) PASTE JOIN (SELECT 2 AS b), (SELECT 3 AS a); -- { serverError ALIAS_REQUIRED }

SELECT '-- The restriction can be disabled entirely';
SELECT brand FROM item, (SELECT s_brand AS brand FROM sales) ORDER BY brand SETTINGS joined_subquery_requires_alias = 0;
SELECT * FROM item, (SELECT toInt32(100) AS brand) ORDER BY item_id SETTINGS joined_subquery_requires_alias = 0 FORMAT TSVWithNames;

SELECT '-- The old analyzer keeps the strict behavior';
SELECT item_id FROM item, (SELECT s_brand AS xbrand FROM sales) WHERE brand = xbrand SETTINGS enable_analyzer = 0; -- { serverError ALIAS_REQUIRED }

DROP TABLE item;
DROP TABLE sales;
DROP TABLE with_number;
DROP TABLE mt;
