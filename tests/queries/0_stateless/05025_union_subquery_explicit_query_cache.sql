-- Tags: no-parallel
-- ^ uses SYSTEM DROP QUERY CACHE, which would interfere with concurrent query cache tests.

-- A subquery opts into the Planner-level query result cache (the `is_subquery = 1` entries) with its own
-- query-level `SETTINGS use_query_cache`. A `UNION` subquery carries that clause on its `UnionNode`, and
-- `Planner::buildPlanForUnionNode` has to honor it the same way `Planner::buildPlanForQueryNode` honors it
-- for a plain subquery - otherwise the explicit opt-in is silently ignored for the union as a whole and
-- only the last arm (which also stores the clause, so that the AST round-trip keeps it) gets cached.

SET allow_experimental_analyzer = 1;

SYSTEM DROP QUERY CACHE TAG '05025_union_in';
SYSTEM DROP QUERY CACHE TAG '05025_union_out';
SYSTEM DROP QUERY CACHE TAG '05025_union_none';

-- Explicit opt-in: the union node itself is cached, in addition to the last arm.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '05025_union_in'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05025_union_in' AND is_subquery = 1;

-- The read path works as well: the second run reuses the same entries instead of adding new ones,
-- and returns the same result.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '05025_union_in'
);
SELECT count() FROM system.query_cache WHERE tag = '05025_union_in' AND is_subquery = 1;

-- Explicit opt-out: nothing is cached, even though the clause is present.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 0, query_cache_min_query_runs = 0, query_cache_tag = '05025_union_out'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05025_union_out' AND is_subquery = 1;

-- No clause at all: `use_query_cache` does not propagate into subqueries by default.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '05025_union_none' FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05025_union_none' AND is_subquery = 1;

SYSTEM DROP QUERY CACHE TAG '05025_union_in';
SYSTEM DROP QUERY CACHE TAG '05025_union_out';
SYSTEM DROP QUERY CACHE TAG '05025_union_none';
