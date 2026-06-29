-- Test: exercises multi-row LineString paths added by PR #62519
-- Covers ColumnToLineStringsConverter::convert offset slicing (geometryConverters.h:103-108)
-- and LineStringSerializer::add cumulative offsets (geometryConverters.h:255-261).
-- The PR's own test 03164_linestring_geometry.sql only tests single-row LineString.

-- Multi-row readWKTLineString → wkt round-trip.
-- Each row has a different number of points, so cumulative offsets must be
-- correct in BOTH the serializer (build) and converter (read) directions.
SELECT wkt(readWKTLineString(s)) AS w FROM (
    SELECT 'LINESTRING (1 1, 2 2)' AS s
    UNION ALL SELECT 'LINESTRING (10 10, 20 20, 30 30, 40 40)' AS s
    UNION ALL SELECT 'LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)' AS s
) ORDER BY w;

-- Multi-row wkt on a LineString-typed table column.
-- Distinct INSERT path (column built directly, not via readWKTLineString),
-- still routed through ColumnToLineStringsConverter on the wkt side.
DROP TABLE IF EXISTS t_linestring_multirow;
CREATE TABLE t_linestring_multirow (id UInt32, ls LineString) ENGINE = Memory;
INSERT INTO t_linestring_multirow VALUES
    (1, [(1, 1), (2, 2)]),
    (2, [(10, 10), (20, 20), (30, 30)]),
    (3, [(0, 0), (1, 0), (1, 1), (0, 1)]),
    (4, []);
SELECT id, wkt(ls), toTypeName(ls) FROM t_linestring_multirow ORDER BY id;
DROP TABLE t_linestring_multirow;
