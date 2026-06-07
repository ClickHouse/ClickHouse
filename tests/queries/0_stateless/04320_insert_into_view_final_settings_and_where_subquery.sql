-- Follow-up review tests for INSERT into regular views (issue #91535).
--
-- 1. `FINAL` changes the read row-set for collapsing/replacing engines but is ignored on the
--    write path, so such views must be rejected at INSERT time.
--
-- 2. A `SETTINGS` clause (e.g. `additional_table_filters`) can change which rows the view reads
--    but is not applied on writes, so such views must be rejected at INSERT time.
--
-- 3. A `WHERE` predicate with a subquery (`a IN (SELECT ...)`) must be enforced as a constraint
--    without throwing a logical error from a not-ready set.

DROP TABLE IF EXISTS t_target;
DROP TABLE IF EXISTS t_allowed;
DROP VIEW IF EXISTS v_final;
DROP VIEW IF EXISTS v_settings;
DROP VIEW IF EXISTS v_where_in;

-- `ReplacingMergeTree` so that `FINAL` is valid at read time and the rejection comes from the
-- view write path (`NOT_IMPLEMENTED`), not from the engine rejecting `FINAL` (`ILLEGAL_FINAL`).
CREATE TABLE t_target (a Int32, b String) ENGINE = ReplacingMergeTree ORDER BY a;
CREATE TABLE t_allowed (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_allowed VALUES (1), (2), (3);

-- 1. FINAL is ignored on the write path -> reject.
CREATE VIEW v_final AS SELECT a, b FROM t_target FINAL;
INSERT INTO v_final VALUES (1, 'x'); -- { serverError NOT_IMPLEMENTED }

-- 2. SETTINGS can change the read row-set but is ignored on the write path -> reject.
CREATE VIEW v_settings AS SELECT a, b FROM t_target SETTINGS additional_table_filters = {'t_target' : 'a > 0'};
INSERT INTO v_settings VALUES (1, 'x'); -- { serverError NOT_IMPLEMENTED }

-- 3. WHERE with a subquery is enforced as a constraint (no logical error from a not-ready set).
CREATE VIEW v_where_in AS SELECT a, b FROM t_target WHERE a IN (SELECT a FROM t_allowed);
INSERT INTO v_where_in VALUES (2, 'ok');   -- a = 2 is allowed
INSERT INTO v_where_in VALUES (5, 'no');   -- { serverError VIOLATED_CONSTRAINT }
SELECT 'where_in:', a, b FROM t_target ORDER BY a;

DROP VIEW v_where_in;
DROP VIEW v_settings;
DROP VIEW v_final;
DROP TABLE t_allowed;
DROP TABLE t_target;
