-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- A nested `SETTINGS profile = ...` clause whose profile sets `use_query_cache` opts the subquery
-- into (or out of) the query result cache the same way a literal `SETTINGS use_query_cache` does.
-- A profile that does not mention `use_query_cache` contributes nothing, so the outer query's
-- `use_query_cache` still does not propagate into the subquery. See #119019.


DROP SETTINGS PROFILE IF EXISTS qc_on_05055;
DROP SETTINGS PROFILE IF EXISTS qc_off_05055;
DROP SETTINGS PROFILE IF EXISTS neutral_05055;
DROP TABLE IF EXISTS t_05055;

CREATE TABLE t_05055 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05055 VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE SETTINGS PROFILE qc_on_05055 SETTINGS use_query_cache = 1;
CREATE SETTINGS PROFILE qc_off_05055 SETTINGS use_query_cache = 0;
CREATE SETTINGS PROFILE neutral_05055 SETTINGS max_threads = 3;

SYSTEM DROP QUERY CACHE;

SELECT '-- a nested profile enabling use_query_cache creates a subquery cache entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x1' SETTINGS profile = 'qc_on_05055');
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- a nested profile followed by an explicit use_query_cache = 0 does not create an entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x2' SETTINGS profile = 'qc_on_05055', use_query_cache = 0);
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- an explicit use_query_cache = 1 followed by a profile that sets it to 0 does not create an entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x3' SETTINGS use_query_cache = 1, profile = 'qc_off_05055');
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- a profile that does not mention use_query_cache does not let the outer use_query_cache propagate';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x4' SETTINGS profile = 'neutral_05055') SETTINGS use_query_cache = 1;
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- under query_cache_for_subqueries, a profile opting out stops the propagation like the literal does';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x5' SETTINGS profile = 'neutral_05055') SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1;
SELECT count() FROM system.query_cache WHERE is_subquery;
SYSTEM DROP QUERY CACHE;
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x6' SETTINGS profile = 'qc_off_05055') SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1;
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- a CTE referenced twice (cloned query tree) still produces a subquery cache entry';
WITH cte AS (SELECT id FROM t_05055 WHERE s != 'x7' SETTINGS profile = 'qc_on_05055')
SELECT a.id, b.id FROM cte AS a, cte AS b WHERE a.id = b.id ORDER BY a.id FORMAT Null;
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

SELECT '-- a UNION subquery opted in through a profile creates the same entries as the literal';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x8' UNION ALL SELECT id FROM t_05055 WHERE s != 'x9' SETTINGS use_query_cache = 1);
SELECT count() FROM system.query_cache WHERE is_subquery;
SYSTEM DROP QUERY CACHE;
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x8' UNION ALL SELECT id FROM t_05055 WHERE s != 'x9' SETTINGS profile = 'qc_on_05055');
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;

DROP TABLE t_05055;
DROP SETTINGS PROFILE qc_on_05055;
DROP SETTINGS PROFILE qc_off_05055;
DROP SETTINGS PROFILE neutral_05055;
