-- The identifier-resolution cache (`enable_identifier_resolve_cache`) must stay engaged
-- for an alias reused inside scalar (literal) `IN` expressions. With the cache enabled,
-- the alias `x` is resolved once and the resolved `plus` node is reused across both `IN`
-- operands (the two `IN` arguments print the same query-tree node id). With the cache
-- disabled, `x` is re-resolved into a distinct node per use. A regression that re-disabled
-- the cache for scalar `IN` (treating a literal-set `IN` as a subquery scope again, as in
-- the bug this guards) would make the two trees below converge and fail this test.

SET enable_analyzer = 1;

SELECT '-- cache disabled: x re-resolved per IN (distinct plus node ids) --';
EXPLAIN QUERY TREE
    SELECT number + 1 AS x, x IN (1, 2, 3) AS a, x IN (4, 5, 6) AS b
    FROM numbers(5)
    SETTINGS enable_identifier_resolve_cache = 0;

SELECT '-- cache enabled: x resolved once, the same plus node is reused across both INs --';
EXPLAIN QUERY TREE
    SELECT number + 1 AS x, x IN (1, 2, 3) AS a, x IN (4, 5, 6) AS b
    FROM numbers(5)
    SETTINGS enable_identifier_resolve_cache = 1;
