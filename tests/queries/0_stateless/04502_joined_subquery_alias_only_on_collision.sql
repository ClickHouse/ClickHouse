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

-- With `prefer_column_name_to_alias = 1` the shadowing goes the other way: the bare identifier binds to
-- the join-tree column and the scope alias does not make it unreachable, so a scope-alias collision does
-- not require the subquery alias in that mode.
WITH 1 AS x SELECT x FROM numbers(1), (SELECT 2 AS x) SETTINGS prefer_column_name_to_alias = 1;

-- An `ARRAY JOIN` expression that is neither aliased nor a plain identifier (here a `COLUMNS(...)` matcher)
-- exposes names that are only known after resolution, so the validation cannot prove the absence of a
-- collision and keeps the strict behavior: the unaliased subquery is rejected even without a provable
-- collision, exactly as when the columns of a table expression cannot be determined.
DROP TABLE IF EXISTS arr_t;
CREATE TABLE arr_t (id UInt8, arr1 Array(UInt8), arr2 Array(UInt8)) ENGINE = Memory;
INSERT INTO arr_t VALUES (1, [10], [20]);

SELECT arr1 FROM arr_t, (SELECT 1 AS arr1) ARRAY JOIN COLUMNS('^arr'); -- { serverError ALIAS_REQUIRED }
SELECT arr1 FROM arr_t, (SELECT 1 AS not_colliding) ARRAY JOIN COLUMNS('^arr'); -- { serverError ALIAS_REQUIRED }

-- With the subquery aliased (or the restriction disabled) the query is accepted.
SELECT arr1 FROM arr_t, (SELECT 1 AS other) AS rhs ARRAY JOIN COLUMNS('^arr');
SELECT arr1 FROM arr_t, (SELECT 1 AS other) ARRAY JOIN COLUMNS('^arr') SETTINGS joined_subquery_requires_alias = 0;

-- An unaliased `ARRAY JOIN` expression that is a compound identifier exposes the name of the column it
-- resolves to, without the table qualifier: `ARRAY JOIN t.arr1` binds the bare name `arr1`. The prepass
-- records every suffix of the identifier, so the collision is seen no matter which leading parts turn out
-- to be the qualifier.
SELECT arr1 FROM arr_t AS t, (SELECT 1 AS arr1) ARRAY JOIN t.arr1; -- { serverError ALIAS_REQUIRED }

-- With the subquery aliased its column stays reachable, so the query is accepted.
SELECT arr1, rhs.arr1 FROM arr_t AS t, (SELECT 1 AS arr1) AS rhs ARRAY JOIN t.arr1;

-- No collision with the name the qualified `ARRAY JOIN` expression binds: still allowed without an alias.
SELECT arr1, other FROM arr_t AS t, (SELECT 1 AS other) ARRAY JOIN t.arr1;

-- Disabling the setting keeps the pre-existing permissive behavior.
SELECT arr1 FROM arr_t AS t, (SELECT 1 AS arr1) ARRAY JOIN t.arr1 SETTINGS joined_subquery_requires_alias = 0;

-- A sibling table expression can itself be wrapped in an `ARRAY JOIN`. Such a sibling exposes the columns
-- of its inner table expression plus the `ARRAY JOIN` output columns, and both sets are already resolved
-- when the join is validated, so it does not force the conservative fallback. No name collides here
-- (`id` / `elem` vs `x`), so the unaliased subquery is allowed.
SELECT id, elem, x FROM (SELECT [0] AS id) AS lhs ARRAY JOIN [1] AS elem INNER JOIN (SELECT 1 AS x) ON true;

-- A collision with the sibling's `ARRAY JOIN` output column still requires the alias ...
SELECT id FROM (SELECT [0] AS id) AS lhs ARRAY JOIN [1] AS elem INNER JOIN (SELECT 1 AS elem) ON true; -- { serverError ALIAS_REQUIRED }

-- ... as does a collision with a column of the table expression inside the sibling's `ARRAY JOIN`.
SELECT elem FROM (SELECT [0] AS id) AS lhs ARRAY JOIN [1] AS elem INNER JOIN (SELECT 1 AS id) ON true; -- { serverError ALIAS_REQUIRED }

-- An `ARRAY JOIN` of a bare column keeps the source column name as the output name; it coincides with
-- the inner column of the same name, so it is not a collision by itself.
SELECT arr1, x FROM arr_t ARRAY JOIN arr1 INNER JOIN (SELECT 1 AS x) ON true;

-- Unlike an *enclosing* `ARRAY JOIN` with a `COLUMNS(...)` matcher (see above), a *sibling* `ARRAY JOIN`
-- is fully resolved by the time the join is validated, so its output names are known even for a matcher
-- and only a real collision requires the alias.
SELECT arr1, arr2, x FROM arr_t ARRAY JOIN COLUMNS('^arr') INNER JOIN (SELECT 1 AS x) ON true;
SELECT arr1 FROM arr_t ARRAY JOIN COLUMNS('^arr') INNER JOIN (SELECT 1 AS arr2) ON true; -- { serverError ALIAS_REQUIRED }

-- The widened (`ALIAS`-including) table function column set is local to the join-alias validation: the
-- `NATURAL JOIN` synthesis keeps matching physical columns only, symmetrically for both operand orders.
-- `z` is a forwarded ALIAS column of `merge`, so neither order synthesizes `USING (z)` and both degrade
-- to a cross join with the same result.
SELECT count() FROM merge(currentDatabase(), '^mt_alias$') AS m NATURAL JOIN (SELECT 2 AS z) AS rhs;
SELECT count() FROM (SELECT 2 AS z) AS lhs NATURAL JOIN merge(currentDatabase(), '^mt_alias$') AS m;

-- A self alias -- an expression alias whose body is the bare identifier of the same name (`x AS x`) --
-- does not shadow the joined column: an alias is skipped while its own body is resolved, so the bare
-- identifier still binds to the join-tree column and reaches it through the alias. No subquery alias
-- is needed.
SELECT x AS x FROM numbers(1), (SELECT 2 AS x);

-- An identifier alias with a different body still shadows: bare `x` binds to the alias (i.e. to
-- `number`), so the subquery output `x` is unreachable without an alias, which is therefore required.
SELECT number AS x FROM numbers(1), (SELECT 2 AS x); -- { serverError ALIAS_REQUIRED }

-- A join operand can itself be a derived table wrapped in `ARRAY JOIN` (in an explicit join the
-- `ARRAY JOIN` binds to the preceding table expression). The `ARRAY JOIN` carries the inner columns
-- through to the enclosing join, so the unaliased inner subquery is validated like a bare one: a
-- collision with a sibling column (`x`) requires the alias.
SELECT x FROM (SELECT [1] AS arr, 2 AS x) ARRAY JOIN arr INNER JOIN (SELECT 0 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }

-- With an alias on the inner derived table the collision is resolvable, so the query is allowed.
SELECT lhs.x, rhs.x, arr FROM (SELECT [1] AS arr, 2 AS x) AS lhs ARRAY JOIN arr INNER JOIN (SELECT 0 AS x) AS rhs ON true;

-- Without a collision the missing alias is harmless, exactly as for a bare derived table.
SELECT y, x, arr FROM (SELECT [1] AS arr, 2 AS x) ARRAY JOIN arr INNER JOIN (SELECT 0 AS y) AS rhs ON true;

-- The `ARRAY JOIN` output alias is part of this side's exposed columns, so it collides with a sibling
-- column of the same name and requires the alias too.
SELECT x FROM (SELECT [1] AS arr) ARRAY JOIN arr AS x INNER JOIN (SELECT 0 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }

-- A plain table wrapped in `ARRAY JOIN` never needs an alias, like a plain table itself, even when its
-- columns collide with a sibling (the table name can always qualify them).
SELECT arr_t.id, rhs.id, elem FROM arr_t ARRAY JOIN arr1 AS elem INNER JOIN (SELECT 0 AS id) AS rhs ON true;

-- Disabling the setting keeps the pre-existing permissive behavior for the wrapped case as well.
SELECT x FROM (SELECT [1] AS arr, 2 AS x) ARRAY JOIN arr INNER JOIN (SELECT 0 AS x) AS rhs ON true SETTINGS joined_subquery_requires_alias = 0;

-- The verdict for a collision with an in-scope expression alias does not depend on the sibling table
-- expressions, so it is reached before they are resolved: the error is `ALIAS_REQUIRED` and not the
-- failure of the unresolvable table function on the right, which is never even looked at.
WITH 1 AS x SELECT x FROM (SELECT 2 AS x) INNER JOIN nonexistent_table_function_04502() ON true; -- { serverError ALIAS_REQUIRED }

-- Without such a collision the verdict does need the sibling columns, so the right operand is resolved
-- first and its own error is reported.
SELECT 1 FROM (SELECT 2 AS y) INNER JOIN nonexistent_table_function_04502() ON true; -- { serverError UNKNOWN_FUNCTION }

-- The same holds for a comma join: an operand that already collides with an operand resolved before it
-- gets its final verdict right there, so the remaining operands are not resolved. Later operands can only
-- add collisions, never remove the one that is already found.
SELECT x FROM (SELECT 0 AS x) AS lhs, (SELECT 1 AS x), nonexistent_table_function_04502(); -- { serverError ALIAS_REQUIRED }

-- Without a collision among the operands resolved so far, the verdict needs the remaining ones, so they
-- are resolved and the unresolvable table function reports its own error.
SELECT x FROM (SELECT 0 AS x) AS lhs, (SELECT 1 AS y), nonexistent_table_function_04502(); -- { serverError UNKNOWN_FUNCTION }

-- A join operand can itself be a join: the comma join nests under the explicit `JOIN` here. The nested
-- join's operands are validated against each other when the nested join is resolved, but a descendant
-- unaliased subquery can also collide with a sibling of an *enclosing* join, which makes its column just
-- as unreachable as in the flat case. The validation descends into nested join operands, so the collision
-- with the enclosing sibling (`x` of `rhs`) requires the alias.
SELECT x FROM (SELECT 1 AS x), numbers(1) JOIN (SELECT 2 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }

-- The collision forces the alias even when the ambiguous name is never referenced bare, exactly as the
-- sibling-level check does (the nested subquery's `x` is unqualifiable either way).
SELECT rhs.x FROM (SELECT 1 AS x), numbers(1) JOIN (SELECT 2 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }

-- With the nested subquery aliased both columns are qualifiable, so the query is allowed.
SELECT lhs.x, rhs.x FROM (SELECT 1 AS x) AS lhs, numbers(1) JOIN (SELECT 2 AS x) AS rhs ON true;

-- Without a collision across the nesting levels the missing alias stays harmless.
SELECT y, x FROM (SELECT 1 AS y), numbers(1) JOIN (SELECT 2 AS x) AS rhs ON true;

-- A parenthesized join is a derived table, not a nested join node: its whole column set participates in
-- the ordinary sibling check, which catches the collision on `x` by itself.
SELECT x FROM ((SELECT 1 AS x) JOIN numbers(1) ON true) JOIN (SELECT 2 AS x) AS rhs ON true; -- { serverError ALIAS_REQUIRED }

-- Disabling the setting keeps the pre-existing permissive behavior for the nested case as well.
SELECT rhs.x FROM (SELECT 1 AS x), numbers(1) JOIN (SELECT 2 AS x) AS rhs ON true SETTINGS joined_subquery_requires_alias = 0;

-- `Nested` columns carry a dot in their name but are real, bindable columns, not sub-columns: a compound
-- identifier `n.x` binds to them just like a bare identifier binds to an ordinary column. When both sides
-- of the join expose `n.x` the reference is genuinely ambiguous, so the alias is required.
DROP TABLE IF EXISTS nested_t;
DROP TABLE IF EXISTS nested_t2;
CREATE TABLE nested_t (id UInt8, n Nested(x UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE nested_t2 (id UInt8, n Nested(x UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO nested_t (id, `n.x`) VALUES (1, [10, 20]);
INSERT INTO nested_t2 (id, `n.x`) VALUES (2, [71, 72]);

SELECT n.x FROM nested_t2, (SELECT n.x FROM nested_t); -- { serverError ALIAS_REQUIRED }

-- With an alias both `n.x` are qualifiable, so the query is allowed.
SELECT sub.n.x FROM nested_t2, (SELECT n.x FROM nested_t) AS sub;

-- ... or when the restriction is disabled entirely.
SELECT n.x FROM nested_t2, (SELECT n.x FROM nested_t) SETTINGS joined_subquery_requires_alias = 0;

-- Without a sibling exposing `n.x` the dotted name does not collide and the missing alias stays harmless.
SELECT n.x FROM (SELECT n.x FROM nested_t), numbers(1);

-- A sub-column of a subquery projection is bindable through a compound identifier too: a projected
-- `Tuple` exposes its elements, so its `n.x` collides with the sibling's `Nested` column `n.x`.
SELECT id FROM nested_t2, (SELECT CAST(tuple(1), 'Tuple(x UInt8)') AS n); -- { serverError ALIAS_REQUIRED }

-- The `_table` virtual of an unaliased table function collides with a sibling's real (non-virtual) column
-- of the same name, even when that sibling is aliased: the aliased sibling exits the validation early, so
-- this orientation is the only one that can catch the shadowing that makes `_table` of `merge(...)`
-- unreachable.
SELECT _table FROM (SELECT '' AS _table) AS rhs, merge(currentDatabase(), '^nested_t$'); -- { serverError ALIAS_REQUIRED }

-- With an alias the table function's `_table` is reachable again.
SELECT m._table FROM (SELECT '' AS _table) AS rhs, merge(currentDatabase(), '^nested_t$') AS m;

-- The ubiquitous `_table` / `_database` virtuals still do not collide with each other (every table
-- expression exposes them), so an unaliased table function with otherwise disjoint columns stays allowed.
SELECT id, number FROM nested_t2, numbers(1);

DROP TABLE nested_t;
DROP TABLE nested_t2;

DROP TABLE item;
DROP TABLE sales;
DROP TABLE with_number;
DROP TABLE mt;
DROP TABLE mt_alias;
DROP TABLE arr_t;
