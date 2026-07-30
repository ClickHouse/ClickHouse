-- The old analyzer does not support the `AS alias(col1, ...)` column alias list at all,
-- so every statement below needs the new analyzer. A session SET also survives the
-- `compatibility` setting randomization, which a no-old-analyzer tag would not.
SET enable_analyzer = 1;

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

-- An IN or EXISTS subquery makes the analyzer put an internal `__subquery_column_<uuid>` name into
-- the same storage the alias list uses. That name must never be re-emitted as a column alias list.
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x) WHERE t.x IN (SELECT 1)) ty;
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x) WHERE EXISTS(SELECT 1)) ty;
SELECT 1 FROM view(SELECT t.p FROM (SELECT 1, 2) t(p, q) WHERE (t.p, t.q) IN (SELECT 1, 2)) ty;

-- A resolved subquery already carries the alias list as projection aliases, and column pruning
-- shrinks those without shrinking the alias list, so re-emitting the list for a resolved subquery
-- would ship a list longer than the projection. These rows send such a subquery to a shard, where
-- the regenerated query is parsed again and the count is rechecked.
DROP TABLE IF EXISTS local_alias_list;
CREATE TABLE local_alias_list (a UInt8) ENGINE = Memory;
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

-- A count mismatch stays an error, inside and outside `view`.
SELECT t.x FROM (SELECT 1) t(x, y); -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1) t(x, y)) ty; -- { serverError BAD_ARGUMENTS }
SELECT 1 FROM view(SELECT t.x FROM (SELECT 1, 2) t(x, x)) ty; -- { serverError BAD_ARGUMENTS }

-- The parser and formatter were never at fault, and a resolved subquery already carries the alias
-- list as a projection alias. Both pin that the fix does not apply the list twice.
SELECT formatQuery('SELECT x FROM (SELECT 1) t(x)');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT x FROM (SELECT 7) t(x);
