-- Tags: no-fasttest

-- Normal hexagon: Polygon type, one ring, closed, and coordinates in (lon, lat) order.
WITH
    toUInt64(599686042433355775) AS h,
    h3CellToPolygon(h) AS polygon,
    h3ToGeoBoundary(h) AS boundary
SELECT
    toTypeName(polygon),
    length(polygon),
    length(polygon[1]) = length(boundary) + 1,
    polygon[1][1] = arrayElement(polygon[1], -1),
    arrayPopBack(polygon[1]) = arrayMap(p -> (p.2, p.1), boundary);

-- WKT compatibility aliases are String and retain all coordinates on a round trip.
WITH
    toUInt64(599686042433355775) AS h,
    h3CellToBoundaryWKT(h) AS boundary_wkt
SELECT
    toTypeName(boundary_wkt),
    startsWith(boundary_wkt, 'POLYGON(('),
    boundary_wkt = h3_cell_to_boundary_wkt(h),
    readWKTPolygon(boundary_wkt) = h3CellToPolygon(h);

-- Both boundary functions ignore the h3ToGeo coordinate-order setting.
SET h3togeo_lon_lat_result_order = 0;

WITH
    toUInt64(599686042433355775) AS h,
    h3CellToPolygon(h) AS polygon
SELECT arrayPopBack(polygon[1]) = arrayMap(p -> (p.2, p.1), h3ToGeoBoundary(h));

SET h3togeo_lon_lat_result_order = 1;

WITH
    toUInt64(599686042433355775) AS h,
    h3CellToPolygon(h) AS polygon
SELECT arrayPopBack(polygon[1]) = arrayMap(p -> (p.2, p.1), h3ToGeoBoundary(h));

-- All 12 resolution-zero pentagons also have closed boundaries.
SELECT
    count() = 12,
    countIf(length(h3CellToPolygon(h)[1]) = length(h3ToGeoBoundary(h)) + 1) = 12
FROM (SELECT arrayJoin(h3GetPentagonIndexes(toUInt8(0))) AS h);

-- The precise WKT boundary should reproduce pentagon vertices as well.
SELECT countIf(readWKTPolygon(h3CellToBoundaryWKT(h)) = h3CellToPolygon(h)) = 12
FROM (SELECT arrayJoin(h3GetPentagonIndexes(toUInt8(0))) AS h);

-- Constants, LowCardinality, and Nullable inputs.
SELECT count() = 2 FROM
(
    SELECT h3CellToPolygon(toLowCardinality(arrayJoin([toUInt64(599686042433355775), toUInt64(599686042433355775)]))) AS polygon
    WHERE length(polygon) = 1
);

SELECT
    h3CellToPolygon(CAST(NULL AS Nullable(UInt64))) = [],
    isNull(h3CellToBoundaryWKT(CAST(NULL AS Nullable(UInt64))));

-- Untyped NULL is Nullable(Nothing), and it also represents an empty polygon.
SELECT h3CellToPolygon(NULL) = [], isNull(h3CellToBoundaryWKT(NULL));

-- A mixed Nullable column must skip validation on NULL rows, not substitute invalid H3 index 0.
SELECT
    count() = 3,
    countIf(length(h3CellToPolygon(h)) = 0) = 1,
    countIf(length(h3CellToPolygon(h)[1]) = 7) = 2,
    countIf(isNull(h3CellToBoundaryWKT(h))) = 1
FROM
(
    SELECT if(number = 1, CAST(NULL AS Nullable(UInt64)), toNullable(toUInt64(599686042433355775))) AS h
    FROM numbers(3)
);

-- An actual invalid index still throws when carried in a Nullable column.
SELECT h3CellToPolygon(CAST(toUInt64(0) AS Nullable(UInt64))); -- { serverError INCORRECT_DATA }
SELECT h3CellToBoundaryWKT(CAST(toUInt64(0) AS Nullable(UInt64))); -- { serverError INCORRECT_DATA }

-- Wrong input types are rejected during function resolution.
SELECT h3CellToPolygon('invalid'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT h3CellToBoundaryWKT('invalid'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Invalid cells throw by default, or produce default values with the setting.
SELECT h3CellToPolygon(toUInt64(0)); -- { serverError INCORRECT_DATA }
SELECT h3CellToBoundaryWKT(toUInt64(0)); -- { serverError INCORRECT_DATA }

SET functions_h3_default_if_invalid = 1;

SELECT
    h3CellToPolygon(toUInt64(0)) = [],
    h3CellToBoundaryWKT(toUInt64(0)) = '';

-- Interleave valid, NULL, and invalid cells in one block with permissive validation.
SELECT
    count() = 4,
    countIf(length(h3CellToPolygon(h)) = 1) = 2,
    countIf(h3CellToPolygon(h) = []) = 2,
    countIf(h3CellToBoundaryWKT(h) = '') = 1,
    countIf(isNull(h3CellToBoundaryWKT(h))) = 1
FROM
(
    SELECT multiIf(
        number = 1, CAST(NULL AS Nullable(UInt64)),
        number = 2, toNullable(toUInt64(0)),
        toNullable(toUInt64(599686042433355775))) AS h
    FROM numbers(4)
);
