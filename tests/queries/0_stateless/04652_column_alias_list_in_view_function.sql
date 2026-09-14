-- The `alias(col1, ...)` column alias list needs the analyzer, and a session SET survives the
-- `compatibility` randomization that a no-old-analyzer tag would not.
SET enable_analyzer = 1;
-- `rewrite_in_to_join` below requires this setting, which the 25.8 block of
-- SettingsChangesHistory.cpp turns on, so a randomized older `compatibility` reverts it.
SET allow_experimental_correlated_subqueries = 1;

-- The `view` table function argument is deliberately excluded from analysis, so the query tree
-- is converted back to AST with no resolved projection column names. The column alias list has
-- to survive that conversion.
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x)) ty;
SELECT 1 FROM view(SELECT x FROM (SELECT 1) t(x)) ty;
SELECT 1 FROM view(SELECT t.p, t.q FROM (SELECT 1, 2) t(p, q)) ty;
SELECT 1 FROM view(SELECT t.p FROM (SELECT 1, 2) t(p, q)) ty;
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1 + 2) t(x)) ty;
SELECT 1 FROM view(SELECT t.x FROM (SELECT dummy FROM system.one) t(x)) ty;
SELECT 1 FROM view(SELECT t.a FROM (SELECT * FROM system.one) t(a)) ty;
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1 UNION ALL SELECT 2) t(x)) ty;
SELECT 1 FROM view(SELECT t.x FROM ((SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3) t(x)) ty;
SELECT 1 FROM view(SELECT t.y FROM (SELECT 1 AS x) t(y)) ty;
SELECT 1 FROM remote('127.0.0.1', view(SELECT t.p FROM (SELECT 1, 2) t(p, q))) ty;
SELECT 1 FROM view(SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x)) tz) ty;
SELECT 1 FROM view(SELECT u.y FROM (SELECT t.x FROM (SELECT 1) t(x)) u(y)) ty;

-- A CTE spells the same list as `WITH name(col1, ...) AS (subquery)` and the builder stores it in
-- the same place, so a conversion back to AST has to restore it there too.
SELECT 1 FROM view(WITH t(x) AS (SELECT 1) SELECT t.x FROM t) ty;
SELECT 1 FROM view(WITH t(x) AS (SELECT 1) SELECT x FROM t) ty;
SELECT 1 FROM view(WITH t(p, q) AS (SELECT 1, 2) SELECT t.p FROM t) ty;
SELECT 1 FROM view(WITH t(p, q) AS (SELECT 1, 2) SELECT t.p, t.q FROM t) ty;
SELECT 1 FROM view(WITH t(y) AS (SELECT 1 AS x) SELECT t.y FROM t) ty;
SELECT 1 FROM remote('127.0.0.1', view(WITH t(x) AS (SELECT 1) SELECT t.x FROM t)) ty;
SELECT 1 FROM view(SELECT 1 FROM view(WITH t(x) AS (SELECT 1) SELECT t.x FROM t) tz) ty;

-- A regenerated body is always a union nested in a union, so a UNION-bodied CTE reaches the
-- nested-union dispatch, which used to drop the list its own caller passed. The nested-union row
-- pins that the list survives the nested-union dispatch, which is where it used to be dropped.
SELECT 1 FROM view(WITH t(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT t.x FROM t) ty;
SELECT 1 FROM view(WITH t(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT x FROM t) ty;
SELECT 1 FROM view(WITH t(p, q) AS (SELECT 1, 2 UNION ALL SELECT 3, 4) SELECT t.p FROM t) ty;
SELECT 1 FROM view(WITH t(x) AS ((SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3) SELECT t.x FROM t) ty;
SELECT 1 FROM remote('127.0.0.1', view(WITH t(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT t.x FROM t)) ty;

-- A recursive CTE names its own alias list from inside its body, so it reads the restored list
-- rather than a qualified reference to it.
SELECT 1 FROM view(WITH RECURSIVE t(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM t WHERE x < 3) SELECT t.x FROM t ORDER BY t.x) ty;
WITH RECURSIVE t(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM t WHERE x < 3) SELECT t.x FROM t ORDER BY t.x;

-- These two read the AST of the `view` argument, which is the one place an unresolved alias list is
-- printed, and assert the list is emitted there while an `IN` rewrite is active. The unresolved-only
-- guard on the emission is pinned separately, by the column-pruning rows further down.
SELECT count() FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
    SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x) WHERE t.x IN (SELECT 1)) ty
    SETTINGS rewrite_in_to_join = 1) WHERE explain LIKE '%) AS t(x)%';
SELECT count() FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
    SELECT 1 FROM view(SELECT t.p FROM (SELECT 1, 2) t(p, q) WHERE (t.p, t.q) IN (SELECT 1, 2)) ty
    SETTINGS rewrite_in_to_join = 1) WHERE explain LIKE '%) AS t(p, q)%';
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x) WHERE t.x IN (SELECT 1)) ty SETTINGS rewrite_in_to_join = 1;
SELECT 1 FROM view(SELECT t.p FROM (SELECT 1, 2) t(p, q) WHERE (t.p, t.q) IN (SELECT 1, 2)) ty SETTINGS rewrite_in_to_join = 1;

-- A resolved subquery already carries the alias list as projection aliases, and column pruning
-- shrinks those without shrinking the alias list, so re-emitting the list for a resolved subquery
-- would ship a list longer than the projection. These rows send such a subquery to a shard, where
-- the regenerated query is parsed again and the count is rechecked.
DROP TABLE IF EXISTS local_alias_list;
CREATE TABLE local_alias_list (a UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO local_alias_list VALUES (1);
SELECT t.p FROM remote('127.0.0.1', currentDatabase(), local_alias_list) d, (SELECT 1, 2) t(p, q) LIMIT 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), local_alias_list) d WHERE d.a IN (SELECT t.p FROM (SELECT 1, 2) t(p, q));
DROP TABLE local_alias_list;

-- Controls: these already worked and must not regress.
SELECT x FROM (SELECT 1) x(x);
SELECT 1 FROM view(SELECT 1 FROM (SELECT 1) t(x)) ty;
SELECT t.x FROM (SELECT 1) t(x);
SELECT t.p, t.q FROM (SELECT 1, 2) t(p, q);
SELECT t.p FROM (SELECT 1, 2) t(p, q);
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1 AS x) t) ty;
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1 AS x) t(x)) ty;
SELECT t.y FROM (SELECT 1 AS x) t(y);
WITH t(x) AS (SELECT 1) SELECT t.x FROM t;
WITH t(p, q) AS (SELECT 1, 2) SELECT t.p FROM t;
WITH t AS (SELECT 1 AS x) SELECT t.x FROM t;
WITH t(x) AS (SELECT 1 UNION ALL SELECT 2) SELECT t.x FROM t ORDER BY t.x;
WITH t(p, q) AS (SELECT 1, 2 UNION ALL SELECT 3, 4) SELECT t.p FROM t ORDER BY t.p;
WITH t(x) AS ((SELECT 1 UNION ALL SELECT 2) UNION ALL SELECT 3) SELECT t.x FROM t ORDER BY t.x;

-- A count mismatch stays an error, inside and outside `view`.
SELECT t.x FROM (SELECT 1) t(x, y); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x, y)) ty; -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1, 2) t(x, x)) ty; -- { serverError BAD_ARGUMENTS }

-- The positional projection name that a dropped alias list used to leave behind is not a supported
-- way to reference the column. Restoring the list replaces it, so `view` now agrees with a plain
-- subquery, where that name has never resolved.
SELECT 1 FROM view(SELECT t.`1` FROM (SELECT 1) t(x)) ty; -- { serverError UNKNOWN_IDENTIFIER }
SELECT t.`1` FROM (SELECT 1) t(x); -- { serverError UNKNOWN_IDENTIFIER }
SELECT 1 FROM view(WITH t(x) AS (SELECT 1) SELECT t.`1` FROM t) ty; -- { serverError UNKNOWN_IDENTIFIER }
WITH t(x) AS (SELECT 1) SELECT t.`1` FROM t; -- { serverError UNKNOWN_IDENTIFIER }

-- The parser and formatter were never at fault, and a resolved subquery already carries the alias
-- list as a projection alias. Both pin that the fix does not apply the list twice.
SELECT formatQuery('SELECT x FROM (SELECT 1) t(x)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT x FROM (SELECT 7) t(x);
