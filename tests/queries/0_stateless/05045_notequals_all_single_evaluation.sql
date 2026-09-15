-- The parser rewrite of `x != ALL (subquery)` must evaluate the right-hand side exactly once:
-- it stays a plain `NOT IN` with a single occurrence of the subquery. In particular there must be
-- no additional emptiness-check subquery, which would re-evaluate the right-hand side - observable
-- when it is non-deterministic - and rescan it for every non-empty right-hand side.
-- The rewrite happens at parse time, so `formatQuery` exposes its shape independently of the analyzer.
SELECT formatQuery('SELECT 1 != ALL (SELECT 1 WHERE 0)');
SELECT 5 != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0));
SELECT 1 != ALL (SELECT 1);
SELECT 1 != ALL (SELECT 2);
SELECT 1 != ALL (SELECT number FROM numbers(3));
