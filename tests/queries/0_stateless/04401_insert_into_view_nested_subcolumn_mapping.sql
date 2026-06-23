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
