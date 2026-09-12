-- Tests that query_cache_use_only_when_data_was_not_changed (issue #108713) sees through CTEs and
-- view-like wrappers: a `WITH` alias must not be mistaken for a base table, and queries reading from
-- a `View` or a `MaterializedView` must track the tables actually holding the data.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache (which would require a no-parallel tag). The
-- system.query_cache checks use table names unique to this test.

DROP TABLE IF EXISTS t_04658_base;
CREATE TABLE t_04658_base (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04658_base VALUES (1), (2);

-- A CTE whose name does not match any real table: the reference must not disable the feature
-- (a failed resolution of the alias used to bail out and skip caching entirely).
SELECT 'cte';
WITH cte_04658 AS (SELECT x FROM t_04658_base) SELECT count() FROM cte_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() > 0 FROM system.query_cache WHERE query LIKE '%t\_04658\_base%';
WITH cte_04658 AS (SELECT x FROM t_04658_base) SELECT count() FROM cte_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
-- The base table read by the CTE body is tracked: an INSERT invalidates the entry.
INSERT INTO t_04658_base VALUES (3);
WITH cte_04658 AS (SELECT x FROM t_04658_base) SELECT count() FROM cte_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- A real table with the same name as the CTE: execution reads the CTE (which shadows the table),
-- so the result comes from t_04658_base, and the consistency key must follow the same resolution.
SELECT 'cte shadowing a real table';
DROP TABLE IF EXISTS shadow_04658;
CREATE TABLE shadow_04658 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO shadow_04658 VALUES (100);
WITH shadow_04658 AS (SELECT x FROM t_04658_base) SELECT sum(x) FROM shadow_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
WITH shadow_04658 AS (SELECT x FROM t_04658_base) SELECT sum(x) FROM shadow_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
-- Still the table behind the CTE that invalidates, not the shadowed one.
INSERT INTO t_04658_base VALUES (4);
WITH shadow_04658 AS (SELECT x FROM t_04658_base) SELECT sum(x) FROM shadow_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
DROP TABLE shadow_04658;

-- A view has no data of its own: the consistency key must be derived from the tables behind its
-- stored SELECT (the entry used to be skipped because a view has no modification hash).
SELECT 'view';
DROP TABLE IF EXISTS v_04658;
CREATE VIEW v_04658 AS SELECT x FROM t_04658_base;
SELECT count() FROM v_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() > 0 FROM system.query_cache WHERE query LIKE '%v\_04658%';
SELECT count() FROM v_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t_04658_base VALUES (5);
SELECT count() FROM v_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
DROP TABLE v_04658;

-- Reading from a materialized view reads its target table, so that is the table to track.
SELECT 'materialized view';
DROP TABLE IF EXISTS mv_04658;
DROP TABLE IF EXISTS mv_src_04658;
CREATE TABLE mv_src_04658 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_04658 ENGINE = MergeTree ORDER BY x AS SELECT x FROM mv_src_04658;
INSERT INTO mv_src_04658 VALUES (1), (2);
SELECT count() FROM mv_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() > 0 FROM system.query_cache WHERE query LIKE '%mv\_04658%';
SELECT count() FROM mv_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO mv_src_04658 VALUES (3);
SELECT count() FROM mv_04658 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
DROP TABLE mv_04658;
DROP TABLE mv_src_04658;

DROP TABLE t_04658_base;
