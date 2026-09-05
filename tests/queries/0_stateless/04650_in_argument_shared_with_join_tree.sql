-- Tags: no-old-analyzer, shard, no-parallel-replicas
-- The fix lives in the analyzer; the old analyzer never resolves an IN argument as a table
-- expression at all, so it rejects these shapes outright and the bug cannot manifest there.

-- A table expression that is also a join-tree table expression of the enclosing query must not be
-- shared with the right argument of an IN-family function: later stages rewrite each argument
-- instance in place (`createUniqueAliasesIfNecessary`, `GLOBAL IN` external tables,
-- `rewrite_in_to_join`), and with a shared node those edits land in the join tree.
-- The measured pre-fix failures for the shapes below are noted per group.
-- (The `exists` consumer of the same resolution site cannot carry this: its argument is always a
-- parsed subquery, never an identifier.)

DROP TABLE IF EXISTS t_04650_l;
DROP TABLE IF EXISTS t_04650_r;
DROP TABLE IF EXISTS t_04650_set;
DROP TABLE IF EXISTS t_04650_mt;

CREATE TABLE t_04650_l (x Int32) ENGINE = Memory;
CREATE TABLE t_04650_r (x Int32) ENGINE = Memory;
INSERT INTO t_04650_l VALUES (1), (2), (3), (4);
INSERT INTO t_04650_r VALUES (2), (3), (9);

-- The join restricts the result to {2, 3}; the ground truth for every IN shape below is therefore
-- 2, 3 and for the NOT IN shape the empty set (checked against the spelled-out subquery form).

SELECT 'ground truth (unshared subquery on the right of IN)';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x IN (SELECT x FROM t_04650_r) ORDER BY l.x;

-- Aborted before the fix with
--   `Logical error: 'Column identifier __table1.x is already registered'`
-- in every one of the clause positions below (all measured; `WHERE` / `HAVING` / `ORDER BY` /
-- `QUALIFY` / `ON` / comma join / inside a lambda were verified, a representative subset is kept
-- here).
SELECT 'WHERE';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x IN r ORDER BY l.x;

SELECT 'QUALIFY';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
QUALIFY l.x IN r ORDER BY l.x;

SELECT 'ORDER BY';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
ORDER BY l.x IN r, l.x;

SELECT 'inside a lambda';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE arrayExists(z -> z IN r, [l.x]) ORDER BY l.x;

SELECT 'NOT IN (empty result)';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x NOT IN r ORDER BY l.x;

-- A table function source: same abort before the fix, reported as `__table1.number`.
SELECT 'table function source';
SELECT t1.number FROM numbers(3) AS t1 INNER JOIN numbers(3) AS t2 ON t1.number = t2.number
WHERE t1.number IN t2 ORDER BY t1.number;

-- `GLOBAL IN` rewrites the argument into an external table. Before the fix, sharing the node with
-- the join tree made the rewrite collide on the generated `FROM` alias:
--   `Code: 179 Duplicate aliases __table2 for table expressions in FROM section are not allowed`
-- An identifier naming a join-tree table expression resolves through two distinct paths, the alias
-- map (`tryResolveIdentifierFromAliases`) and the table-expression lookup
-- (`tryResolveIdentifierFromTableExpression`), so all three spellings are exercised. Each of the
-- three aborted with `Code: 179` before the fix.
SELECT 'GLOBAL IN, subquery alias';
SELECT t1.dummy FROM remote('127.0.0.1', system.one) AS t1
INNER JOIN system.one AS t2 ON t1.dummy = t2.dummy
WHERE t1.dummy GLOBAL IN t2;

SELECT 'GLOBAL IN, bare one-part table name';
SELECT t1.x FROM remote('127.0.0.1', currentDatabase(), t_04650_r) AS t1
INNER JOIN t_04650_l ON t1.x = t_04650_l.x
WHERE t1.x GLOBAL IN t_04650_l ORDER BY t1.x;

SELECT 'GLOBAL IN, two-part db.table name';
SELECT t1.dummy FROM remote('127.0.0.1', system.one) AS t1
INNER JOIN system.one AS t2 ON t1.dummy = t2.dummy
WHERE t1.dummy GLOBAL IN system.one;

-- The `rewrite_in_to_join` rewrite resolves the argument on its own path. Before the fix that path
-- produced `Code: 10 Columns [__table2.dummy] are not found in blocks [__table1.dummy], []`.
SELECT 'rewrite_in_to_join';
SELECT t1.dummy FROM system.one AS t1
INNER JOIN (SELECT * FROM system.one) AS t2 ON t1.dummy = t2.dummy
WHERE t1.dummy IN t2
SETTINGS rewrite_in_to_join = 1, allow_experimental_correlated_subqueries = 1;

-- Shapes below are CONTROLS: each already produced its expected value before the fix, so they pin
-- that the fix did not widen behaviour.
SELECT 'control: CTE (the pre-existing clone path)';
WITH c AS (SELECT x FROM t_04650_r)
SELECT l.x FROM t_04650_l AS l INNER JOIN c ON l.x = c.x WHERE l.x IN c ORDER BY l.x;

SELECT 'control: IN names the LEFT subquery';
SELECT l.x FROM (SELECT x FROM t_04650_l) AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x IN l ORDER BY l.x;

SELECT 'control: bare table alias on the right of IN';
SELECT l.x FROM t_04650_l AS l INNER JOIN t_04650_r AS r ON l.x = r.x WHERE l.x IN r ORDER BY l.x;

SELECT 'control: union subquery alias';
SELECT l.x FROM t_04650_l AS l
INNER JOIN (SELECT x FROM t_04650_r UNION ALL SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x IN r ORDER BY l.x;

SELECT 'control: single table expression, no join';
SELECT x FROM (SELECT x FROM t_04650_r) AS r WHERE x IN r ORDER BY x;

SELECT 'control: GLOBAL IN over an unshared subquery';
SELECT t1.dummy FROM remote('127.0.0.1', system.one) AS t1
INNER JOIN system.one AS t2 ON t1.dummy = t2.dummy
WHERE t1.dummy GLOBAL IN (SELECT dummy FROM system.one);

SELECT 'control: tuple IN';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE (l.x, l.x) IN (SELECT x, x FROM t_04650_r) ORDER BY l.x;

-- The old analyzer never resolves an IN argument as a table expression at all, so it rejects the
-- shape outright. Measured identical before and after the fix.
SELECT 'control: old analyzer rejects the shape';
SELECT l.x FROM t_04650_l AS l INNER JOIN (SELECT x FROM t_04650_r) AS r ON l.x = r.x
WHERE l.x IN r ORDER BY l.x SETTINGS enable_analyzer = 0; -- { serverError UNKNOWN_TABLE }

-- A `Set`-engine table on the right of IN is looked up by tree hash in `CollectSets` and is never the
-- shared join-tree node, so the clone cannot reach it.
CREATE TABLE t_04650_set (x Int32) ENGINE = Set;
INSERT INTO t_04650_set VALUES (2), (3);
SELECT 'control: Set engine on the right of IN';
SELECT x FROM t_04650_l WHERE x IN t_04650_set ORDER BY x;
SELECT l.x FROM t_04650_l AS l INNER JOIN t_04650_l AS l2 ON l.x = l2.x
WHERE l.x IN t_04650_set ORDER BY l.x;

-- The shapes below come from the same sharing reached through `QUALIFY` on a `MergeTree` source;
-- before the fix they aborted with `Logical error: 'No set is registered for key ...'`.
CREATE TABLE t_04650_mt (y Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO t_04650_mt SELECT number FROM numbers(5);

SELECT 'QUALIFY source, IN the same source';
SELECT y FROM (SELECT y FROM t_04650_mt QUALIFY 1) AS t_04650_mt WHERE y IN t_04650_mt ORDER BY y;
SELECT 'QUALIFY source, NOT IN the same source';
SELECT count() FROM (SELECT y FROM t_04650_mt QUALIFY 1) AS t_04650_mt WHERE y NOT IN t_04650_mt;
SELECT 'QUALIFY source, GLOBAL IN the same source';
SELECT y FROM (SELECT y FROM t_04650_mt QUALIFY 1) AS src WHERE y GLOBAL IN src ORDER BY y;
SELECT 'QUALIFY source, globalNotIn in QUALIFY';
SELECT count() FROM (SELECT y FROM t_04650_mt QUALIFY 1) AS t_04650_mt
QUALIFY globalNotIn((SELECT 1), t_04650_mt);

-- `SortNode::cloneImpl` must carry `column_name`: it is derived from the original AST identifier
-- rather than from a child node, so the base clone machinery drops it. The planner puts it into
-- `SortColumnDescription::alias`, which is the name `FillingTransform` matches against the
-- `INTERPOLATE` outputs, so without it a clone silently loses the `ORDER BY` / `INTERPOLATE`
-- conflict check. The uncloned spellings of the query below already raise the error, so the clone
-- must too.
SELECT 'clone fidelity of SortNode::column_name, uncloned spelling';
SELECT x AS a FROM t_04650_l ORDER BY a WITH FILL FROM 1 TO 5 STEP 1 INTERPOLATE (a AS a); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT 'clone fidelity of SortNode::column_name, CTE clone';
WITH c AS (SELECT x AS a FROM t_04650_l ORDER BY a WITH FILL FROM 1 TO 5 STEP 1 INTERPOLATE (a AS a))
SELECT * FROM c; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT 'clone fidelity of SortNode::column_name, IN-argument clone';
SELECT l.x FROM t_04650_l AS l
INNER JOIN (SELECT x AS a FROM t_04650_l ORDER BY a WITH FILL FROM 1 TO 5 STEP 1 INTERPOLATE (a AS a)) AS r
  ON l.x = r.a
WHERE l.x IN r ORDER BY l.x; -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT 'control: INTERPOLATE a column other than the fill key';
WITH c AS (SELECT x AS a, x * 10 AS y FROM t_04650_l ORDER BY a WITH FILL FROM 1 TO 5 STEP 1 INTERPOLATE (y AS y))
SELECT * FROM c ORDER BY a;
-- `column_name` also reaches `EXPLAIN` through `SortNode::dumpTreeImpl`, so the clone is pinned a
-- second, error-independent way: without the clone fix the CTE copy prints a bare `EXPRESSION`.
SELECT 'clone fidelity of SortNode::column_name, visible in EXPLAIN';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1
  WITH c AS (SELECT x AS a FROM t_04650_l ORDER BY a WITH FILL FROM 1 TO 9 STEP 1) SELECT * FROM c
) WHERE explain ILIKE '%EXPRESSION a%';

DROP TABLE t_04650_mt;
DROP TABLE t_04650_set;
DROP TABLE t_04650_r;
DROP TABLE t_04650_l;
