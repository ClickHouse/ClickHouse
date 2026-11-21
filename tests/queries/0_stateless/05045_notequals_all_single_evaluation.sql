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

-- A NULL left-hand side follows the NULL semantics of `notIn`: under the default `transform_null_in = 0` the
-- result is NULL even for an empty right-hand side, whether the NULL is a literal, a constant of a Nullable
-- type, or a NULL value inside a Nullable column. Only `transform_null_in = 1` makes an empty right-hand side
-- TRUE for NULL as well. This is unlike the aggregate-based `= ALL` rewrite, where an empty right-hand side is
-- TRUE regardless of the left-hand side.
SELECT NULL != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0));
SELECT CAST(NULL, 'Nullable(UInt8)') != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0));
SELECT number, nullIf(number, 0) != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0)) FROM numbers(2);
SELECT NULL != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0)) SETTINGS transform_null_in = 1;
SELECT number, nullIf(number, 0) != ALL (SELECT x FROM (SELECT 1 AS x WHERE 0)) FROM numbers(2) SETTINGS transform_null_in = 1;
SELECT NULL != ALL (SELECT 1);
SELECT NULL = ALL (SELECT x FROM (SELECT 1 AS x WHERE 0));
