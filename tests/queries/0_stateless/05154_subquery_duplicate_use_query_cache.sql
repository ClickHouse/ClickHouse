-- Tags: no-parallel
-- ^ uses SYSTEM DROP QUERY CACHE, which would interfere with concurrent query cache tests.

-- A subquery opts into the Planner-level query result cache (the `is_subquery = 1` entries) with its own
-- query-level `SETTINGS use_query_cache`. `ParserSetQuery` keeps every occurrence of a setting and the
-- clause is applied in order, so the effective value is the last one. The Planner must therefore read the
-- effective value from the node's context rather than the first `use_query_cache` entry of the clause,
-- otherwise `use_query_cache = 1, use_query_cache = 0` takes the cache path for a subquery that opted out.

SET allow_experimental_analyzer = 1;

SYSTEM DROP QUERY CACHE TAG '05154_last_wins_out';
SYSTEM DROP QUERY CACHE TAG '05154_last_wins_in';
SYSTEM DROP QUERY CACHE TAG '05154_reset_out';
SYSTEM DROP QUERY CACHE TAG '05154_union_out';

-- Last occurrence wins: the opt-out is the effective value, nothing is cached.
SELECT count() FROM
(
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 1, use_query_cache = 0, query_cache_min_query_runs = 0, query_cache_tag = '05154_last_wins_out'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05154_last_wins_out' AND is_subquery = 1;

-- ... and the other way round: the opt-in is the effective value, the subquery is cached.
SELECT count() FROM
(
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 0, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '05154_last_wins_in'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05154_last_wins_in' AND is_subquery = 1;

-- A `= DEFAULT` after an assignment resets the setting back to the (disabled) default in the subquery scope.
SELECT count() FROM
(
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 1, use_query_cache = DEFAULT, query_cache_min_query_runs = 0, query_cache_tag = '05154_reset_out'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05154_reset_out' AND is_subquery = 1;

-- The same holds for a `UNION` subquery, whose clause is carried by the `UnionNode`.
SELECT count() FROM
(
    SELECT number AS x FROM numbers(2)
    UNION ALL
    SELECT number FROM numbers(3)
    SETTINGS use_query_cache = 1, use_query_cache = 0, query_cache_min_query_runs = 0, query_cache_tag = '05154_union_out'
) FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05154_union_out' AND is_subquery = 1;

SYSTEM DROP QUERY CACHE TAG '05154_last_wins_out';
SYSTEM DROP QUERY CACHE TAG '05154_last_wins_in';
SYSTEM DROP QUERY CACHE TAG '05154_reset_out';
SYSTEM DROP QUERY CACHE TAG '05154_union_out';
