-- Tests for mvtEncodeGeom geometry clipping (the buffer and clip arguments) and the tile bounding-box
-- helpers mvtTileBBox / mvtTileBBoxMercator (and the ST_TileEnvelope alias).

SELECT '-- mvtTileBBox: bounding box of the whole world at zoom 0';
SELECT mvtTileBBox(0, 0, 0);

SELECT '-- mvtTileBBoxMercator: bounding box of a tile in Web Mercator space';
SELECT mvtTileBBoxMercator(1, 0, 0);

SELECT '-- ST_TileEnvelope is an alias for mvtTileBBoxMercator';
SELECT ST_TileEnvelope(1, 0, 0) = mvtTileBBoxMercator(1, 0, 0);

SELECT '-- mvtTileBBox: a positive margin expands the box on every side';
WITH mvtTileBBox(10, 550, 335) AS bb, mvtTileBBox(10, 550, 335, 0.1) AS bm
SELECT bm.1 < bb.1, bm.2 < bb.2, bm.3 > bb.3, bm.4 > bb.4;

SELECT '-- mvtTileBBox round-trips with mvtEncodeGeom: the tile centre projects to an interior pixel';
WITH mvtTileBBox(10, 550, 335) AS bb
SELECT mvtEncodeGeom(((bb.1 + bb.3) / 2, (bb.2 + bb.4) / 2)::Point, 10, 550, 335);

SELECT '-- mvtEncodeGeom: clip=true (the default) returns NULL for an out-of-tile point';
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 10, 550, 335) IS NULL;

SELECT '-- mvtEncodeGeom: clip=false keeps an out-of-tile point (projected, not clipped)';
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 10, 550, 335, 4096, 256, false) IS NOT NULL;

SELECT '-- mvtEncodeGeom: buffer 0 clips a point ~5px past the edge that buffer 16 keeps';
WITH
    mvtTileBBox(10, 550, 335) AS bb,
    (bb.3 - bb.1) / 4096 AS deg_per_px,
    bb.3 + deg_per_px * 5 AS lon_just_outside,
    (bb.2 + bb.4) / 2 AS lat_mid
SELECT
    mvtEncodeGeom((lon_just_outside, lat_mid)::Point, 10, 550, 335, 4096, 0) IS NULL AS buf0_clips,
    mvtEncodeGeom((lon_just_outside, lat_mid)::Point, 10, 550, 335, 4096, 16) IS NOT NULL AS buf16_keeps;

SELECT '-- mvtEncodeGeom: a polygon covering the whole tile is clipped to the tile box (buffer 0)';
SELECT mvtEncodeGeom([[(13.0, 52.0), (14.0, 52.0), (14.0, 53.0), (13.0, 53.0), (13.0, 52.0)]]::Polygon, 10, 550, 335, 4096, 0);

SELECT '-- mvtEncodeGeom: a line crossing the tile edge is clipped to the buffer boundary';
SELECT mvtEncodeGeom([(13.5, 52.6), (20.0, 52.6)]::LineString, 10, 550, 335, 4096, 0);

SELECT '-- mvtEncode: a geometry coordinate outside the representable MVT range is rejected';
SELECT mvtEncode('t')((3000000000.0, 0.0)::Point::Geometry); -- { serverError BAD_ARGUMENTS }

SELECT '-- tile indices must be < 2^zoom in mvtEncodeGeom, mvtTileBBox and mvtTileBBoxMercator';
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 0, 1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 1, 0, 2); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mvtTileBBox(0, 1, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mvtTileBBoxMercator(0, 0, 1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- the largest valid tile index for a zoom is accepted';
SELECT mvtTileBBox(1, 1, 1) IS NOT NULL, mvtEncodeGeom((13.37, 52.52)::Point, 1, 1, 0, 4096, 0, false) IS NOT NULL;
