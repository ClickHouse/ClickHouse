-- Regression test for INSERT into a regular view that projects a Nested subcolumn.
-- The view-list identifier must be resolved against the underlying table so that a compound
-- subcolumn name like `n.x` is preserved and only a table/database/alias qualifier is stripped.
-- Collapsing `n.x` to its short name `x` would route the inserted values into the unrelated
-- plain column `x`. See PR #102866 (https://github.com/ClickHouse/ClickHouse/pull/102866#discussion_r3435713221).

DROP TABLE IF EXISTS t_nested_sub;
DROP TABLE IF EXISTS v_nested_sub;
DROP TABLE IF EXISTS v_qualified_plain;

-- The target table has both a plain column `x` and a `Nested` subcolumn `n.x` of the same type,
-- so a wrong mapping would silently write into the other column instead of being rejected on type.
CREATE TABLE t_nested_sub
(
    x Array(UInt8),
    n Nested(x UInt8)
)
ENGINE = MergeTree ORDER BY tuple();

-- Projecting the Nested subcolumn `n.x` (renamed via an alias).
CREATE VIEW v_nested_sub AS SELECT n.x AS nx FROM t_nested_sub;

INSERT INTO v_nested_sub VALUES ([1, 2, 3]);

-- The value must land in `n.x`; the plain column `x` keeps its default (empty array).
SELECT 'nested subcolumn', x, n.x FROM t_nested_sub;

-- A table-qualified reference to the plain column must still have its qualifier stripped and
-- map to `x` (and not be confused with the Nested subcolumn `n.x`).
TRUNCATE TABLE t_nested_sub;
CREATE VIEW v_qualified_plain AS SELECT t_nested_sub.x AS xx FROM t_nested_sub;

INSERT INTO v_qualified_plain VALUES ([7, 8]);

SELECT 'qualified plain', x, n.x FROM t_nested_sub;

DROP TABLE v_qualified_plain;
DROP TABLE v_nested_sub;
DROP TABLE t_nested_sub;

-- A subcolumn rename whose alias collides with a *different* top-level column is ambiguous when the
-- view's WHERE references the colliding name: `SELECT n.x AS x ... WHERE ... x ...` maps the alias
-- `x` to the underlying `n.x`, but a real top-level `x` exists too, so the constraint has no single
-- read semantics. The alias-collision guard must reject this (it must compare the alias with its
-- resolved target name `n.x`, not with `shortName()` which would collapse `n.x` to `x` and skip it).
DROP TABLE IF EXISTS t_collide;
DROP VIEW IF EXISTS v_collide;
CREATE TABLE t_collide (x UInt8, n Nested(x UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW v_collide AS SELECT n.x AS x FROM t_collide WHERE toString(x) != '';
INSERT INTO v_collide VALUES ([1, 2, 3]); -- { serverError NOT_IMPLEMENTED }
DROP VIEW v_collide;
DROP TABLE t_collide;

-- The same subcolumn rename is fine when no top-level column collides with the alias: here the
-- underlying table has `y` and `n.x` but no top-level `x`, so `SELECT n.x AS x ... WHERE ... x ...`
-- is unambiguous and the INSERT is accepted (the guard must not over-reject).
DROP TABLE IF EXISTS t_nocollide;
DROP VIEW IF EXISTS v_nocollide;
CREATE TABLE t_nocollide (y UInt8, n Nested(x UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW v_nocollide AS SELECT n.x AS x FROM t_nocollide WHERE toString(x) != '';
INSERT INTO v_nocollide VALUES ([4, 5]);
SELECT 'no collision', y, n.x FROM t_nocollide;
DROP VIEW v_nocollide;
DROP TABLE t_nocollide;
