-- Tags: no-parallel
-- no-parallel: a settings profile is server-global rather than per-database, and its name cannot be
-- made unique per run: query parameters are not accepted in access-entity DDL. So this test is not
-- safe against a concurrent copy of itself - which is how the flaky check runs it.

-- A nested `SETTINGS profile = ...` clause that enables `use_query_cache` through the profile must
-- opt the subquery into the query result cache the same way a literal `SETTINGS use_query_cache = 1`
-- does. A profile that does not mention `use_query_cache` must not let the outer query's
-- `use_query_cache` propagate into the subquery - the no-propagation rule still holds. See #119019.

SET enable_analyzer = 1;

DROP SETTINGS PROFILE IF EXISTS qc_on_05055;
DROP SETTINGS PROFILE IF EXISTS qc_off_05055;
DROP SETTINGS PROFILE IF EXISTS mdp_off_05055;
DROP TABLE IF EXISTS t_05055;

CREATE TABLE t_05055 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05055 VALUES (1, 'a'), (2, 'b'), (3, 'c');

CREATE SETTINGS PROFILE qc_on_05055 SETTINGS use_query_cache = 1;
CREATE SETTINGS PROFILE qc_off_05055 SETTINGS use_query_cache = 0;
CREATE SETTINGS PROFILE mdp_off_05055 SETTINGS make_distributed_plan = 0;

SYSTEM DROP QUERY CACHE;

SELECT '-- a nested profile enabling use_query_cache creates a subquery cache entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x1' SETTINGS profile = 'qc_on_05055');
SELECT count(*) FROM system.query_cache;
-- Expected: 1

SYSTEM DROP QUERY CACHE;

SELECT '-- a nested profile followed by an explicit use_query_cache = 0 does not create an entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x2' SETTINGS profile = 'qc_on_05055', use_query_cache = 0);
SELECT count(*) FROM system.query_cache;
-- Expected: 0

SYSTEM DROP QUERY CACHE;

SELECT '-- an explicit use_query_cache = 1 followed by a profile that sets it to 0 does not create an entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x3' SETTINGS use_query_cache = 1, profile = 'qc_off_05055');
SELECT count(*) FROM system.query_cache;
-- Expected: 0

SYSTEM DROP QUERY CACHE;

SELECT '-- a neutral profile (no use_query_cache mention), no outer use_query_cache anywhere, does not create an entry';
SELECT count() FROM (SELECT id FROM t_05055 WHERE s != 'x4' SETTINGS profile = 'mdp_off_05055');
SELECT count(*) FROM system.query_cache;
-- Expected: 0

SYSTEM DROP QUERY CACHE;

SELECT '-- normalization: profile before make_distributed_plan still ends with the same adjustment as today';
SELECT count() FROM (
    SELECT id, getSetting('compile_expressions') AS ce FROM t_05055
    WHERE s != 'x5' SETTINGS profile = 'mdp_off_05055', make_distributed_plan = 1, compile_expressions = 1
) WHERE ce = 0;
-- Expected: 3 (compile_expressions was forced false by adjustSettingsForMakeDistributedPlan)

SELECT '-- normalization: reverse order still gives today''s result, proving no normalization point moved';
SELECT count() FROM (
    SELECT id, getSetting('compile_expressions') AS ce, getSetting('make_distributed_plan') AS mdp FROM t_05055
    WHERE s != 'x6' SETTINGS make_distributed_plan = 1, compile_expressions = 1, profile = 'mdp_off_05055'
) WHERE ce = 1 AND mdp = 0;
-- Expected: 3

SYSTEM DROP QUERY CACHE;

SELECT '-- a CTE referenced twice (cloned query tree) still produces subquery cache entries';
WITH cte AS (SELECT id FROM t_05055 WHERE s != 'x7' SETTINGS profile = 'qc_on_05055')
SELECT a.id, b.id FROM cte AS a, cte AS b WHERE a.id = b.id ORDER BY a.id FORMAT Null;
SELECT count(*) FROM system.query_cache;
-- Expected: 1

SYSTEM DROP QUERY CACHE;

SELECT '-- EXPLAIN QUERY TREE output is unchanged by a profile-opted-in subquery';
EXPLAIN QUERY TREE SELECT count() FROM (SELECT id FROM t_05055 SETTINGS profile = 'qc_on_05055') FORMAT Null;

DROP TABLE t_05055;
DROP SETTINGS PROFILE qc_on_05055;
DROP SETTINGS PROFILE qc_off_05055;
DROP SETTINGS PROFILE mdp_off_05055;