-- Tags: no-fasttest
-- Test: argument-type validation paths in `h3PolygonToCells`.
-- Covers: `src/Functions/h3PolygonToCells.cpp` — the first argument's geometry kind is refused from
-- `getReturnTypeImpl`, during analysis, with `ILLEGAL_TYPE_OF_ARGUMENT`.
-- Covers: `src/Functions/h3PolygonToCells.cpp` — `ILLEGAL_COLUMN` when the second argument's column
-- is not `ColumnUInt8`, which is still decided while executing.

-- Argument 1 = Point, a geometry kind the function refuses. Rejected from the argument type alone,
-- so it no longer depends on the column reaching `executeImpl`.
SELECT h3PolygonToCells((1.0, 2.0)::Point, 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Same via a stored Point column: the rejection is bound to the type, not to the column.
DROP TABLE IF EXISTS t_point_04145;
CREATE TABLE t_point_04145 (p Point) ENGINE = Memory;
INSERT INTO t_point_04145 VALUES ((1.0, 2.0));
SELECT h3PolygonToCells(p, 7) FROM t_point_04145; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE t_point_04145;

-- Argument 2 = String (not UInt8) -> ILLEGAL_COLUMN. Reaches the second checkAndGetColumn after `convertToFullColumnIfConst`.
SELECT h3PolygonToCells([(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)], 'abc'); -- { serverError ILLEGAL_COLUMN }
