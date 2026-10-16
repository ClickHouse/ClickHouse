-- A query-construction setting reset to DEFAULT (`SETTINGS limit = DEFAULT`) lives in the AST's
-- `default_settings`, not `changes`, so `hasConstructionSettings` must also look there. Otherwise a
-- `CREATE VIEW … SETTINGS limit = DEFAULT` or an `ALTER TABLE … MODIFY QUERY … SETTINGS filter = DEFAULT`
-- would slip an unsupported construction-setting form past the stored-view guard, which the PR otherwise
-- rejects for the `= value` variants. Companion of 04367_construction_settings_in_view.

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
DROP VIEW IF EXISTS v_ok;

CREATE TABLE t_src (x UInt64) ENGINE = MergeTree ORDER BY x AS SELECT number FROM numbers(10);
CREATE TABLE t_dst (x UInt64) ENGINE = MergeTree ORDER BY x;

SELECT '-- a `= DEFAULT` construction setting in a VIEW definition is rejected too';
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS limit = DEFAULT; -- { serverError NOT_IMPLEMENTED }
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS filter = DEFAULT; -- { serverError NOT_IMPLEMENTED }
-- a value followed by a `= DEFAULT` reset is still a construction setting on the definition
CREATE VIEW v_bad AS SELECT x FROM t_src SETTINGS limit = 3, limit = DEFAULT; -- { serverError NOT_IMPLEMENTED }

SELECT '-- a `= DEFAULT` construction setting in a MATERIALIZED VIEW definition is rejected too';
CREATE MATERIALIZED VIEW mv_bad TO t_dst AS SELECT x FROM t_src SETTINGS limit = DEFAULT; -- { serverError NOT_IMPLEMENTED }

SELECT '-- a `= DEFAULT` construction setting via ALTER TABLE ... MODIFY QUERY is rejected too';
CREATE MATERIALIZED VIEW mv_alter TO t_dst AS SELECT x FROM t_src;
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM t_src SETTINGS filter = DEFAULT; -- { serverError NOT_IMPLEMENTED }
-- ... including when it hides in a nested subquery's own SETTINGS
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM (SELECT x FROM t_src SETTINGS limit = DEFAULT); -- { serverError NOT_IMPLEMENTED }
-- a MODIFY QUERY without construction settings still works
ALTER TABLE mv_alter MODIFY QUERY SELECT x FROM t_src WHERE x > 5;
DROP TABLE mv_alter;

SELECT '-- a plain view works; a `= DEFAULT` reset on the reading query is applied there (no limit, 10 rows)';
CREATE VIEW v_ok AS SELECT x FROM t_src;
SELECT count() FROM v_ok SETTINGS limit = DEFAULT;

DROP VIEW v_ok;
DROP TABLE t_dst;
DROP TABLE t_src;
