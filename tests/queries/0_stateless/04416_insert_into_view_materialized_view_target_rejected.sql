-- Follow-up review test for INSERT into regular views (issue #91535).
--
-- A regular view whose FROM clause references another view must not be insertable, and this holds
-- for a materialized view target just like for a regular view target. The intermediate view carries
-- no column `DEFAULT`s of the final target table, so an omitted column would be materialized as a
-- type default and the final storage's `DEFAULT` expression would never apply: forwarding a full
-- `INSERT INTO mv (a, b)` would hide the omitted `b` from `t`'s `DEFAULT 42` and store `b = 0`.
-- Such targets are rejected with `NOT_IMPLEMENTED` (via `isView`).

DROP VIEW IF EXISTS v_over_mv;
DROP VIEW IF EXISTS v_over_table;
DROP TABLE IF EXISTS mv_target;
DROP TABLE IF EXISTS t_target;
DROP TABLE IF EXISTS t_source;

CREATE TABLE t_target (a UInt8, b UInt8 DEFAULT 42) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_source (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv_target TO t_target AS SELECT a, b FROM t_source;

-- A direct regular view over the real table is insertable and applies the target DEFAULT.
CREATE VIEW v_over_table AS SELECT a, b FROM t_target;
INSERT INTO v_over_table (a) VALUES (1);
SELECT 'direct:', a, b FROM t_target ORDER BY a;

-- A regular view whose target is a materialized view is rejected.
CREATE VIEW v_over_mv AS SELECT a, b FROM mv_target;
INSERT INTO v_over_mv (a) VALUES (2); -- { serverError NOT_IMPLEMENTED }

-- The rejection holds for a full column list too.
INSERT INTO v_over_mv (a, b) VALUES (2, 7); -- { serverError NOT_IMPLEMENTED }

-- Nothing leaked into the underlying tables through the rejected view.
SELECT 'after_rejected:', a, b FROM t_target ORDER BY a;

DROP VIEW v_over_mv;
DROP VIEW v_over_table;
DROP TABLE mv_target;
DROP TABLE t_target;
DROP TABLE t_source;
