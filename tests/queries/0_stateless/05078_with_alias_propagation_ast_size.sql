-- With the old analyzer, `ApplyWithAliasVisitor` propagates every visible `WITH` alias by cloning it
-- into every descendant subquery. An alias whose subquery contains another `SELECT` therefore expands
-- again at each level, so `k` such aliases nested `d` levels deep produce `k ^ d` nodes. `EXPLAIN`
-- supplies that extra level, and three aliases were already enough to exhaust the server's memory
-- rather than fail. The expansion is now bounded like alias substitution in `QueryNormalizer`.

SET enable_analyzer = 0;

-- Three aliases: the expansion crosses `max_expanded_ast_elements` and must be reported, not allocated.
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e3
SELECT e1, e2, e3; -- { serverError TOO_BIG_AST }

-- Below the limit the query is unaffected.
SELECT 'two aliases still work';
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2
SELECT e1 > 0, e2 > 0;

-- A plain alias is not affected by the bound at all.
SELECT 'plain aliases unaffected';
WITH (SELECT count() FROM numbers(10)) AS e1,
     (SELECT count() FROM numbers(10)) AS e2,
     (SELECT count() FROM numbers(10)) AS e3
SELECT e1, e2, e3;

-- The new analyzer does not clone aliases per subquery and handles all of it.
SELECT 'new analyzer unaffected';
WITH (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e1,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e2,
     (SELECT count() FROM (EXPLAIN PLAN SELECT 1)) AS e3
SELECT e1 > 0, e2 > 0, e3 > 0 SETTINGS enable_analyzer = 1;
