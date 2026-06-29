-- Query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` / `page`)
-- shape a result by wrapping the query as a derived table during direct execution. They are NOT supported
-- in a stored view definition: a view's columns are inferred before any wrapping (so `select` would change
-- the result schema versus the stored metadata), the per-`UNION`-arm pass is not applied, and a refreshable
-- materialized view refreshes through `InterpreterInsertQuery` rather than `executeQuery`. They are
-- rejected at CREATE time; specify them on the query that reads the view instead.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
DROP VIEW IF EXISTS v_ok;

CREATE TABLE t_src (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = MergeTree ORDER BY x;

SELECT '-- construction settings in a VIEW definition are rejected';
CREATE VIEW v_bad AS SELECT x FROM t_src ORDER BY x SETTINGS limit = 3; -- { serverError NOT_IMPLEMENTED }
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS filter = 'x >= 7'; -- { serverError NOT_IMPLEMENTED }
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS sort = '-x', limit = 2; -- { serverError NOT_IMPLEMENTED }
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS select = 'x'; -- { serverError NOT_IMPLEMENTED }

SELECT '-- construction settings in a MATERIALIZED VIEW definition are rejected';
CREATE MATERIALIZED VIEW mv_bad TO t_dst AS SELECT x FROM t_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }

SELECT '-- a POPULATE materialized view is rejected too (not silently accepted via the immediate-insert wrapping)';
CREATE MATERIALIZED VIEW mv_pop ENGINE = MergeTree ORDER BY x POPULATE AS SELECT x FROM t_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }

SELECT '-- construction settings via ALTER TABLE ... MODIFY QUERY are rejected (cannot bypass the CREATE guard)';
CREATE MATERIALIZED VIEW mv_alter TO t_dst AS SELECT x FROM t_src;
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM t_src SETTINGS limit = 1; -- { serverError NOT_IMPLEMENTED }
-- the construction setting can hide in a nested subquery's own SETTINGS; reject that too
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM (SELECT x FROM t_src SETTINGS limit = 1); -- { serverError NOT_IMPLEMENTED }
-- a MODIFY QUERY without construction settings still works
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM t_src WHERE x > 5;
DROP TABLE mv_alter;

SELECT '-- a plain view works; the reader applies construction settings to its own SELECT';
CREATE VIEW v_ok AS SELECT x FROM t_src;
SELECT x FROM v_ok ORDER BY x SETTINGS limit = 3;
SELECT x FROM v_ok ORDER BY x SETTINGS filter = 'x >= 7';

DROP VIEW v_ok;
DROP TABLE t_dst;
DROP TABLE t_src;
