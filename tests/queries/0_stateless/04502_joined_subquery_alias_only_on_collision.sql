-- With joined_subquery_requires_alias = 1 (the default) an unaliased subquery or table function in a
-- join is only rejected when it exposes a column whose name also occurs in another table expression of
-- the same join: only then is an unqualified reference ambiguous with no way to qualify it. When there
-- is no such collision the missing alias is harmless and the query is allowed.

-- This relaxation lives in the analyzer only; the deprecated non-analyzer path keeps the strict behavior.
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

-- A table function such as `merge` forwards the ALIAS columns of its source tables, and a bare identifier
-- can bind to them, so an unaliased subquery whose output collides with such a forwarded ALIAS column is
-- ambiguous and requires an alias. The collision check therefore uses the full bindable column set (as the
-- binder does), not just physical columns; previously the forwarded ALIAS column was ignored and this
-- slipped through.
CREATE TABLE mt_alias (id UInt8, z UInt8 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt_alias (id) VALUES (1), (2);

SELECT z FROM merge(currentDatabase(), '^mt_alias$'), (SELECT 1 AS z) AS rhs; -- { serverError ALIAS_REQUIRED }

-- An aliased sibling makes the reference qualifiable again, so the query is accepted.
SELECT m.z FROM merge(currentDatabase(), '^mt_alias$') AS m, (SELECT 1 AS z) AS rhs ORDER BY m.z;

-- No collision with the forwarded ALIAS column: still allowed without an alias.
SELECT z FROM merge(currentDatabase(), '^mt_alias$'), (SELECT 1 AS not_z) AS rhs ORDER BY z;

-- Sibling table expressions are not the only bare-identifier binders: an in-scope expression alias
-- (`WITH` or projection alias) shadows join-tree columns by default, so a subquery output colliding
-- with such an alias is unreachable unless the subquery is aliased. The alias is therefore required.
WITH 1 AS x SELECT x FROM numbers(1), (SELECT 2 AS x); -- { serverError ALIAS_REQUIRED }

-- With the subquery aliased, the shadowed column becomes reachable again via qualification.
WITH 1 AS x SELECT x, rhs.x FROM numbers(1), (SELECT 2 AS x) AS rhs;

-- A projection alias shadows the same way as a `WITH` alias.
SELECT number + 10 AS y FROM numbers(1), (SELECT 2 AS y); -- { serverError ALIAS_REQUIRED }

-- No collision with the scope alias: still allowed without an alias.
WITH 1 AS x SELECT x, not_x FROM numbers(1), (SELECT 2 AS not_x);

-- ... and the restriction can still be disabled entirely.
WITH 1 AS x SELECT x FROM numbers(1), (SELECT 2 AS x) SETTINGS joined_subquery_requires_alias = 0;

-- An enclosing `ARRAY JOIN` alias is a bare-identifier binder as well: it shadows join-tree columns and is
-- resolved before them, so a joined subquery output colliding with an `ARRAY JOIN` alias is only reachable
-- when the subquery is aliased. The alias is therefore required (the `ARRAY JOIN` aliases are registered in
-- the scope only after the inner join tree is validated, so they are tracked separately).
SELECT a FROM numbers(1), (SELECT 2 AS a) ARRAY JOIN [30] AS a; -- { serverError ALIAS_REQUIRED }

-- With the subquery aliased, the shadowed column becomes reachable again via qualification.
SELECT a, rhs.a FROM numbers(1), (SELECT 2 AS a) AS rhs ARRAY JOIN [30] AS a;

-- No collision with the `ARRAY JOIN` alias: still allowed without an alias.
SELECT a FROM numbers(1), (SELECT 2 AS not_a) ARRAY JOIN [30] AS a;

-- ... and the restriction can still be disabled entirely.
SELECT a FROM numbers(1), (SELECT 2 AS a) ARRAY JOIN [30] AS a SETTINGS joined_subquery_requires_alias = 0;

DROP TABLE item;
DROP TABLE sales;
DROP TABLE with_number;
DROP TABLE mt;
DROP TABLE mt_alias;
