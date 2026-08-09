-- Tests for review feedback on INSERT into regular views (issue #91535).
--
-- 1. When the user lists an explicit subset of view columns in the INSERT,
--    omitted columns must receive the *target table's* default, not the view-schema
--    default that the framework would otherwise fill in.
-- 2. A view whose SELECT list references a `WITH` alias (or any other identifier
--    that is not a column of the underlying table) is not insertable.
-- 3. The view's WHERE predicate is evaluated against the values that will actually
--    be written to the target table — i.e. after the target table's column DEFAULTs
--    have been applied to columns the user omitted from a partial INSERT.
-- 4. `OFFSET` (with or without `LIMIT`) is not a simple projection and is rejected.

DROP TABLE IF EXISTS t_target;
DROP VIEW IF EXISTS v_star;
DROP VIEW IF EXISTS v_aliased;
DROP VIEW IF EXISTS v_with;
DROP VIEW IF EXISTS v_where_default;
DROP VIEW IF EXISTS v_where_default_aliased;
DROP VIEW IF EXISTS v_offset;
DROP VIEW IF EXISTS v_offset_only;

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

-- 3. WHERE is evaluated against target-table defaults for omitted columns.
DROP TABLE IF EXISTS t_default;
CREATE TABLE t_default (a Int32, b Int32 DEFAULT 10) ENGINE = MergeTree ORDER BY a;

-- 3a. WHERE references `b`, which has `DEFAULT 10`. A partial INSERT that omits `b`
--     must succeed because the target default (10) satisfies `b > 0`, not because
--     of the type default (0) which would fail.
CREATE VIEW v_where_default AS SELECT * FROM t_default WHERE b > 0;
INSERT INTO v_where_default (a) VALUES (1);
SELECT 'where_default_partial:', a, b FROM t_default ORDER BY a;

-- 3b. Same scenario but the column is aliased — the mapping must still resolve
--     the target column's default for the predicate.
CREATE VIEW v_where_default_aliased AS SELECT a AS id, b AS amount FROM t_default WHERE b > 0;
INSERT INTO v_where_default_aliased (id) VALUES (2);
SELECT 'where_default_aliased:', a, b FROM t_default ORDER BY a;

-- 3c. Full INSERT — the user value (-5) does not satisfy the predicate and must be rejected.
INSERT INTO v_where_default VALUES (3, -5); -- { serverError VIOLATED_CONSTRAINT }

-- 4. OFFSET is rejected the same way as LIMIT.
CREATE VIEW v_offset AS SELECT a, b FROM t_target LIMIT 5 OFFSET 3;
INSERT INTO v_offset VALUES (100, 'x'); -- { serverError NOT_IMPLEMENTED }

CREATE VIEW v_offset_only AS SELECT a, b FROM t_target OFFSET 3;
INSERT INTO v_offset_only VALUES (101, 'y'); -- { serverError NOT_IMPLEMENTED }

DROP VIEW v_offset_only;
DROP VIEW v_offset;
DROP VIEW v_where_default_aliased;
DROP VIEW v_where_default;
DROP TABLE t_default;
DROP VIEW v_with;
DROP VIEW v_aliased;
DROP VIEW v_star;
DROP TABLE t_target;
