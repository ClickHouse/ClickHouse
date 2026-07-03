-- With joined_subquery_requires_alias = 1 (the default) an unaliased subquery or table function in a
-- join is only rejected when it exposes a column whose name also occurs in another table expression of
-- the same join: only then is an unqualified reference ambiguous with no way to qualify it. When there
-- is no such collision the missing alias is harmless and the query is allowed.

DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS with_number;

CREATE TABLE item (item_id Int32, brand Int32) ENGINE = Memory;
CREATE TABLE sales (s_item Int32, s_brand Int32) ENGINE = Memory;
CREATE TABLE with_number (number Int32) ENGINE = Memory;
INSERT INTO item VALUES (1, 100), (2, 200);
INSERT INTO sales VALUES (10, 100), (20, 999);

-- No collision: allowed (comma join).
SELECT item_id FROM item, (SELECT s_brand AS xbrand FROM sales) WHERE brand = xbrand ORDER BY item_id;

-- No collision: allowed (explicit JOIN ... ON).
SELECT item_id FROM item JOIN (SELECT s_item, s_brand FROM sales) ON brand = s_brand ORDER BY item_id;

-- No collision with a table function.
SELECT item_id FROM item, numbers(3) WHERE item_id = number ORDER BY item_id;

-- No collision with a UNION/INTERSECT subquery.
SELECT item_id FROM item, (SELECT s_brand AS xb FROM sales INTERSECT SELECT s_brand FROM sales) WHERE brand = xb ORDER BY item_id;

-- Collision on `brand`: an alias is required.
SELECT item_id FROM item, (SELECT s_brand AS brand FROM sales); -- { serverError ALIAS_REQUIRED }

-- Collision on `brand` in an explicit JOIN ... ON: an alias is required.
SELECT item_id FROM item JOIN (SELECT s_item AS item_id, s_brand AS brand FROM sales) ON item.brand = brand; -- { serverError ALIAS_REQUIRED }

-- Collision on `number` with a table function: an alias is required.
SELECT count() FROM with_number, numbers(3); -- { serverError ALIAS_REQUIRED }

-- The restriction can still be disabled entirely, even in the presence of a collision.
SELECT item_id FROM item, (SELECT s_brand AS brand FROM sales) ORDER BY item_id SETTINGS joined_subquery_requires_alias = 0;

DROP TABLE item;
DROP TABLE sales;
DROP TABLE with_number;
