-- Tags: no-fasttest
-- Test: argument-type validation paths in `h3PolygonToCells`.
-- Covers: src/Functions/h3PolygonToCells.cpp:93-96 — `ILLEGAL_COLUMN` when arg 1 column is not `ColumnArray`.
-- Covers: src/Functions/h3PolygonToCells.cpp:100-103 — `ILLEGAL_COLUMN` when arg 2 column is not `ColumnUInt8`.
-- Both error paths are uncovered per LLVM (lines 94-96 and 101-103) and the PR rewrites them.

-- Argument 1 = Point (Tuple, not Array) -> ILLEGAL_COLUMN.
SELECT h3PolygonToCells((1.0, 2.0)::Point, 7); -- { serverError ILLEGAL_COLUMN }

-- Same via a stored Point column to exercise the non-const branch.
DROP TABLE IF EXISTS t_point_04145;
CREATE TABLE t_point_04145 (p Point) ENGINE = Memory;
INSERT INTO t_point_04145 VALUES ((1.0, 2.0));
SELECT h3PolygonToCells(p, 7) FROM t_point_04145; -- { serverError ILLEGAL_COLUMN }
DROP TABLE t_point_04145;

-- Argument 2 = String (not UInt8) -> ILLEGAL_COLUMN. Reaches the second checkAndGetColumn after `convertToFullColumnIfConst`.
SELECT h3PolygonToCells([(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)], 'abc'); -- { serverError ILLEGAL_COLUMN }
