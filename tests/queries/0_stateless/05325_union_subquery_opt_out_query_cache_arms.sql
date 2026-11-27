-- Tags: no-parallel
-- ^ uses SYSTEM DROP QUERY CACHE, which would interfere with concurrent query cache tests.

-- A set-operation subquery that explicitly opts out of the Planner-level query result cache with its own
-- `SETTINGS use_query_cache = 0` must not be cached through the outer `query_cache_for_subqueries`
-- propagation either - not even its earlier arms, which do not carry the clause themselves (only the last
-- arm does, where the parser leaves a trailing clause).

SET allow_experimental_analyzer = 1;

SYSTEM DROP QUERY CACHE TAG '05325_union_opt_out';

SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 0, query_cache_min_query_runs = 0, query_cache_tag = '05325_union_opt_out'
)
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, query_cache_min_query_runs = 0, query_cache_tag = '05325_union_opt_out'
FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05325_union_opt_out' AND is_subquery = 1;

-- An explicit `use_query_cache = 1` on an arm still wins.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2) SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '05325_union_opt_out'
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 0
)
SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, query_cache_min_query_runs = 0, query_cache_tag = '05325_union_opt_out'
FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05325_union_opt_out' AND is_subquery = 1;

SYSTEM DROP QUERY CACHE TAG '05325_union_opt_out';
