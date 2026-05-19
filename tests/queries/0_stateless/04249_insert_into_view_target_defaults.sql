-- Tests for review feedback on INSERT into regular views (issue #91535).
--
-- 1. When the user lists an explicit subset of view columns in the INSERT,
--    omitted columns must receive the *target table's* default, not the view-schema
--    default that the framework would otherwise fill in.
-- 2. A view whose SELECT list references a `WITH` alias (or any other identifier
--    that is not a column of the underlying table) is not insertable.

DROP TABLE IF EXISTS t_target;
DROP VIEW IF EXISTS v_star;
DROP VIEW IF EXISTS v_aliased;
DROP VIEW IF EXISTS v_with;

CREATE TABLE t_target (a Int32, b String DEFAULT 'target_b', c Float64 DEFAULT 0.5) ENGINE = MergeTree ORDER BY a;

-- 1a. INSERT INTO v_star (a) — `b` and `c` must use t_target's defaults, not type defaults.
CREATE VIEW v_star AS SELECT * FROM t_target;
INSERT INTO v_star (a) VALUES (1);
SELECT 'star_partial:', a, b, c FROM t_target ORDER BY a;
TRUNCATE TABLE t_target;

-- 1b. INSERT INTO v_aliased (id) — `name` is aliased from `b`; `c` is not projected.
--     Both should get t_target's defaults.
CREATE VIEW v_aliased AS SELECT a AS id, b AS name FROM t_target;
INSERT INTO v_aliased (id) VALUES (2);
SELECT 'aliased_partial:', a, b, c FROM t_target ORDER BY a;
TRUNCATE TABLE t_target;

-- 1c. Full INSERT INTO v_star — all defaults must be overridden by user-provided values.
INSERT INTO v_star (a, b, c) VALUES (3, 'user_b', 9.9);
SELECT 'star_full:', a, b, c FROM t_target ORDER BY a;
TRUNCATE TABLE t_target;

-- 2. A view with a `WITH` clause is rejected at INSERT time.
CREATE VIEW v_with AS WITH a + 1 AS x SELECT x FROM t_target;
INSERT INTO v_with VALUES (10); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_with;
DROP VIEW v_aliased;
DROP VIEW v_star;
DROP TABLE t_target;
