-- With joined_subquery_requires_alias = 1 (the default) an unaliased subquery or table function in a
-- join is only rejected when it exposes a column whose name also occurs in another table expression of
-- the same join: only then is an unqualified reference ambiguous with no way to qualify it. When there
-- is no such collision the missing alias is harmless and the query is allowed.

-- This relaxation lives in the new analyzer only; the old analyzer keeps the strict behavior.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS with_number;
DROP TABLE IF EXISTS mt;

CREATE TABLE item (item_id Int32, brand Int32) ENGINE = Memory;
CREATE TABLE sales (s_item Int32, s_brand Int32) ENGINE = Memory;
CREATE TABLE with_number (number Int32) ENGINE = Memory;
CREATE TABLE mt (id UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt VALUES (1), (2);
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

-- Virtual columns are bindable identifiers too: a subquery output colliding with a sibling's virtual
-- column `_part` is just as ambiguous as one colliding with an ordinary column, so an alias is required.
SELECT id FROM mt, (SELECT '' AS _part); -- { serverError ALIAS_REQUIRED }

-- The same query is accepted once the subquery is aliased (the ambiguity can then be qualified away).
SELECT id FROM mt, (SELECT '' AS _part) AS sub ORDER BY id;

-- ... or when the restriction is disabled.
SELECT id FROM mt, (SELECT '' AS _part) ORDER BY id SETTINGS joined_subquery_requires_alias = 0;

-- No collision with a non-virtual, non-ordinary name: still allowed without an alias.
SELECT id FROM mt, (SELECT 1 AS not_a_column) ORDER BY id;

-- A table function exposes its own virtual columns too, not just the sibling's. An unaliased table
-- function whose virtual `_part` collides with an aliased sibling's column is ambiguous, so an alias is
-- required (previously the unaliased side's own virtual columns were ignored and this slipped through).
SELECT _part FROM merge(currentDatabase(), '^mt$'), (SELECT '' AS _part) AS rhs; -- { serverError ALIAS_REQUIRED }

-- The ubiquitous `_table` / `_database` virtuals are the exception: they are exposed by every table
-- expression, so they never count for the unaliased side (otherwise this unaliased table function would
-- collide with `item`'s identically-named virtuals). Nothing else collides, so the query is allowed.
SELECT count() FROM merge(currentDatabase(), '^mt$'), item;

DROP TABLE item;
DROP TABLE sales;
DROP TABLE with_number;
DROP TABLE mt;
