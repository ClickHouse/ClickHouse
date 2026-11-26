-- Tags: no-fasttest
-- ^ polygon clipping uses the wagyu contrib, which is disabled in the fast-test (ENABLE_LIBRARIES=0) build.
-- Tests for MVTEncodeGeom geometry clipping (the buffer and clip arguments) and the tile bounding-box
-- helpers MVTBoundingBox / MVTBoundingBoxMercator.

SELECT '-- MVTBoundingBox: bounding box of the whole world at zoom 0';
SELECT MVTBoundingBox(0, 0, 0);

SELECT '-- MVTBoundingBoxMercator: bounding box of a tile in Web Mercator space';
SELECT MVTBoundingBoxMercator(1, 0, 0);

SELECT '-- MVTBoundingBoxMercator: the last tile spans exactly 2^(32 - zoom) Mercator units on each axis (no off-by-one)';
SELECT z, (bb.3 - bb.1) = bitShiftLeft(toUInt64(1), 32 - z) AS width_ok
FROM (SELECT z, MVTBoundingBoxMercator(z, t, t) AS bb FROM (SELECT CAST(arrayJoin([1, 16, 24, 31]), 'UInt8') AS z, CAST(bitShiftLeft(toUInt64(1), z) - 1, 'UInt32') AS t))
ORDER BY z;

SELECT '-- MVTBoundingBox: a positive margin expands the box on every side';
WITH MVTBoundingBox(10, 550, 335) AS bb, MVTBoundingBox(10, 550, 335, 0.1) AS bm
SELECT bm.1 < bb.1, bm.2 < bb.2, bm.3 > bb.3, bm.4 > bb.4;

SELECT '-- MVTBoundingBox: a negative or non-finite margin is rejected (it would invert the bounds)';
SELECT MVTBoundingBox(0, 0, 0, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT MVTBoundingBox(0, 0, 0, nan); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- MVTBoundingBox: a Nullable tile control is rejected (its non-nullable Tuple return cannot represent NULL)';
SELECT MVTBoundingBox(CAST(NULL, 'Nullable(UInt8)'), 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT MVTBoundingBoxMercator(10, 550, 335, CAST(NULL, 'Nullable(Float64)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTBoundingBox round-trips with MVTEncodeGeom: the tile centre projects to an interior pixel';
WITH MVTBoundingBox(10, 550, 335) AS bb
SELECT MVTEncodeGeom(((bb.1 + bb.3) / 2, (bb.2 + bb.4) / 2)::Point, 10, 550, 335);

SELECT '-- MVTEncodeGeom: clip=true (the default) returns NULL for an out-of-tile point';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 10, 550, 335) IS NULL;

SELECT '-- MVTEncodeGeom: clip=false keeps an out-of-tile point (projected, not clipped)';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 10, 550, 335, 4096, 256, false) IS NOT NULL;

SELECT '-- MVTEncodeGeom: buffer 0 clips a point ~5px past the edge that buffer 16 keeps';
WITH
    MVTBoundingBox(10, 550, 335) AS bb,
    (bb.3 - bb.1) / 4096 AS deg_per_px,
    bb.3 + deg_per_px * 5 AS lon_just_outside,
    (bb.2 + bb.4) / 2 AS lat_mid
SELECT
    MVTEncodeGeom((lon_just_outside, lat_mid)::Point, 10, 550, 335, 4096, 0) IS NULL AS buf0_clips,
    MVTEncodeGeom((lon_just_outside, lat_mid)::Point, 10, 550, 335, 4096, 16) IS NOT NULL AS buf16_keeps;

SELECT '-- MVTEncodeGeom: a polygon covering the whole tile is clipped to the tile box (buffer 0)';
SELECT MVTEncodeGeom([[(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 53.0), (13.0, 52.0)]]::Polygon, 10, 550, 335, 4096, 0);

SELECT '-- MVTEncodeGeom: a line crossing the tile edge is clipped to the buffer boundary';
SELECT MVTEncodeGeom([(13.5, 52.6), (20.0, 52.6)]::LineString, 10, 550, 335, 4096, 0);

SELECT '-- MVTEncodeGeom: empty geometries yield NULL rather than an empty container';
SELECT MVTEncodeGeom([]::LineString, 2, 2, 1, 4096, 0, 0) IS NULL;
SELECT MVTEncodeGeom([]::Polygon, 2, 2, 1, 4096, 0, 0) IS NULL;
SELECT MVTEncodeGeom([[]]::MultiPolygon, 2, 2, 1, 4096, 0, 0) IS NULL;

SELECT '-- MVTEncode: a geometry coordinate outside the representable MVT range is rejected';
SELECT MVTEncode('t')((3000000000.0, 0.0)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- MVTEncode: a geometry delta exceeding the MVT command range is rejected (both vertices are in range)';
SELECT MVTEncode('t')([(-2147483648.0, 0.0), (2147483647.0, 0.0)]::LineString::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- tile indices must be < 2^zoom in MVTEncodeGeom, MVTBoundingBox and MVTBoundingBoxMercator';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 0, 1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 1, 0, 2); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT MVTBoundingBox(0, 1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT MVTBoundingBoxMercator(0, 0, 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- MVTEncodeGeom: an extent + 2 * buffer that cannot fit the MVT command stream is rejected when clipping';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 2147483647, 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- MVTEncodeGeom: the extent + 2 * buffer limit does not apply when clipping is disabled (buffer is unused)';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 2147483647, 1, 0) IS NOT NULL;

SELECT '-- MVTEncodeGeom: fractional (non-integer) tile controls are rejected rather than silently truncated';
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 1.5, 0, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT MVTEncodeGeom((0.0, 0.0)::Point, 10, 550, 335, 4096.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- MVTEncodeGeom: a NULL argument yields a NULL geometry instead of a spurious default-position feature';
SELECT MVTEncodeGeom((13.37, 52.52)::Point, CAST(NULL, 'Nullable(UInt8)'), 0, 0) IS NULL;
SELECT MVTEncodeGeom((13.37, 52.52)::Point, CAST(10, 'Nullable(UInt8)'), 550, 335) IS NOT NULL;

SELECT '-- the largest valid tile index for a zoom is accepted';
SELECT MVTBoundingBox(1, 1, 1) IS NOT NULL, MVTEncodeGeom((13.37, 52.52)::Point, 1, 1, 0, 4096, 0, false) IS NOT NULL;

SELECT '-- MVTEncodeGeom: a point that rounds onto the tile edge is kept (snapped to the grid before clipping)';
SELECT MVTEncodeGeom((90.005, 30.0)::Point, 2, 2, 1, 4096, 0);

SELECT '-- MVTEncodeGeom: a sub-pixel line straddling a tile edge clips to a single edge line, not disjoint fragments';
SELECT MVTEncodeGeom([(0.005, 10.0), (-0.005, 20.0), (-0.006, 30.0), (0.004, 40.0), (0.005, 50.0)]::LineString, 2, 2, 1, 4096, 0);

SELECT '-- MVTEncodeGeom: a self-intersecting ring inside the tile is repaired into valid polygons (it was dropped)';
SELECT wkt(MVTEncodeGeom([[(10.0, 10.0), (50.0, 50.0), (10.0, 50.0), (50.0, 10.0), (10.0, 10.0)]]::Polygon, 2, 2, 1));

SELECT '-- MVTEncodeGeom: a self-crossing sliver clips to a small valid polygon, not a whole-tile fill';
SELECT wkt(MVTEncodeGeom([[(15.403, 0.667), (15.335, 0.28), (15.398, -0.186), (15.331, 0.374), (15.403, 0.667)]]::Polygon, 2, 2, 1));

SELECT '-- MVTEncodeGeom: a self-intersecting ring crossing the tile is repaired into two valid polygons (it was emitted as one self-intersecting ring)';
SELECT wkt(MVTEncodeGeom([[(-10.0, -5.0), (100.0, 60.0), (-10.0, 60.0), (100.0, -5.0), (-10.0, -5.0)]]::Polygon, 2, 2, 1));

SELECT '-- MVTEncodeGeom: a polygon far larger than the tile renders its in-tile portion at high zoom (clipped to the tile box)';
SELECT wkt(MVTEncodeGeom([[(-179.0, -85.0), (179.0, -85.0), (179.0, 85.0), (-179.0, 85.0), (-179.0, -85.0)]]::Polygon, 24, 8388608, 8388608));

SELECT '-- MVTEncodeGeom: a polygon entirely outside the tile becomes NULL';
SELECT MVTEncodeGeom([[(100.0, 10.0), (150.0, 50.0), (100.0, 50.0), (150.0, 10.0), (100.0, 10.0)]]::Polygon, 2, 0, 0) IS NULL;

SELECT '-- MVTEncodeGeom: a polygon with a hole keeps the hole through clipping';
SELECT wkt(MVTEncodeGeom([[(13.3, 52.4), (13.7, 52.4), (13.7, 52.7), (13.3, 52.7), (13.3, 52.4)], [(13.45, 52.5), (13.55, 52.5), (13.55, 52.6), (13.45, 52.6), (13.45, 52.5)]]::Polygon, 10, 550, 335));

SELECT '-- MVTEncodeGeom: clip=false still validates a self-intersecting polygon through wagyu (repaired into valid polygons, like PostGIS)';
SELECT wkt(MVTEncodeGeom([[(10.0, 10.0), (50.0, 50.0), (10.0, 50.0), (50.0, 10.0), (10.0, 10.0)]]::Polygon, 2, 2, 1, 4096, 0, false));

SELECT '-- MVTEncodeGeom: clip=false validates but does not clip to the tile (the repaired geometry keeps its out-of-tile coordinates)';
SELECT wkt(MVTEncodeGeom([[(-10.0, -5.0), (100.0, 60.0), (-10.0, 60.0), (100.0, -5.0), (-10.0, -5.0)]]::Polygon, 2, 2, 1, 4096, 0, false));
