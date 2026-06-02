-- Tests for the Mapbox Vector Tile encoding functions: mvtEncodeGeom (scalar geometry projection) and
-- mvtEncode (aggregate tile assembly). The projection is Web Mercator; the tile format follows the
-- Mapbox Vector Tile specification 2.1.

SELECT '-- mvtEncodeGeom: project a known point into tile-local pixel space';
SELECT mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335);

SELECT '-- mvtEncodeGeom: the return type is Geometry';
SELECT toTypeName(mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335));

SELECT '-- mvtEncodeGeom: ST_AsMVTGeom is an alias';
SELECT ST_AsMVTGeom((13.37, 52.52)::Point, 10, 550, 335) = mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335);

SELECT '-- mvtEncodeGeom: out-of-range longitude/latitude are clamped to the projection limits';
-- clip=false so the comparison tests the projection, not whether the (out-of-tile) point survives clipping.
SELECT mvtEncodeGeom((200.0, 50.0)::Point, 5, 10, 10, 4096, 256, false) = mvtEncodeGeom((180.0, 50.0)::Point, 5, 10, 10, 4096, 256, false);
SELECT mvtEncodeGeom((20.0, 100.0)::Point, 5, 10, 10, 4096, 256, false) = mvtEncodeGeom((20.0, 85.05112877980659)::Point, 5, 10, 10, 4096, 256, false);

SELECT '-- mvtEncodeGeom: a larger extent yields proportionally larger pixel coordinates';
SELECT mvtEncodeGeom((13.37, 52.52)::Point, 10, 550, 335, 8192);

SELECT '-- mvtEncodeGeom: a line is projected and returned as a MultiLineString';
SELECT mvtEncodeGeom([(13.4, 52.5), (13.5, 52.6), (13.6, 52.55)]::LineString, 10, 550, 335);

SELECT '-- mvtEncodeGeom: a polygon is projected and returned as a MultiPolygon';
SELECT mvtEncodeGeom([[(13.4, 52.5), (13.65, 52.5), (13.65, 52.65), (13.4, 52.65), (13.4, 52.5)]]::Polygon, 10, 550, 335);

SELECT '-- mvtEncodeGeom: out-of-range zoom and zero extent are errors';
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 33, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT mvtEncodeGeom((0.0, 0.0)::Point, 0, 0, 0, 0); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The properties argument must be a named tuple; aliases inside tuple(...) are dropped, so names are set with a cast.

SELECT '-- mvtEncode: a single point feature with one uint attribute is a valid single-layer tile';
SELECT hex(mvtEncode('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64)));

SELECT '-- mvtEncode: ST_AsMVT is an alias';
SELECT ST_AsMVT('points')((124.0, 3384.0)::Point::Geometry) = mvtEncode('points')((124.0, 3384.0)::Point::Geometry);

SELECT '-- mvtEncode: geometry only, no properties';
SELECT hex(mvtEncode('points')((124.0, 3384.0)::Point::Geometry));

-- A single feature per tile is used here: mvtEncode is order-dependent, so the exact bytes of a multi-feature tile
-- depend on the order rows reach the aggregate (not stable under parallelism), which would make the test flaky.
SELECT '-- mvtEncode: a line feature';
SELECT hex(mvtEncode('shapes')(mvtEncodeGeom([(13.4, 52.5), (13.5, 52.6)]::LineString, 10, 550, 335)));

SELECT '-- mvtEncode: a polygon feature';
SELECT hex(mvtEncode('shapes')(mvtEncodeGeom([[(13.4, 52.5), (13.65, 52.5), (13.65, 52.65), (13.4, 52.65), (13.4, 52.5)]]::Polygon, 10, 550, 335)));

SELECT '-- mvtEncode: an empty group produces an empty tile';
SELECT length(mvtEncode('points')((0.0, 0.0)::Point::Geometry)) FROM numbers(0);

SELECT '-- mvtEncode: NULL (clipped-out) geometry rows are skipped, so they do not change the tile';
SELECT
(
    SELECT length(mvtEncode('points')(geom))
    FROM
    (
        SELECT mvtEncodeGeom((13.5, 52.6)::Point, 10, 550, 335) AS geom
        UNION ALL SELECT mvtEncodeGeom((0.0, 0.0)::Point, 10, 550, 335)
    )
) = length(mvtEncode('points')(mvtEncodeGeom((13.5, 52.6)::Point, 10, 550, 335)));

DROP TABLE IF EXISTS mvt_points;
CREATE TABLE mvt_points (lon Float64, lat Float64, name String) ENGINE = Memory;
INSERT INTO mvt_points VALUES (13.37, 52.52, 'a') (13.37, 52.52, 'b') (13.40, 52.50, 'c');

SELECT '-- mvtEncode: clustering is expressed in SQL by grouping on the pixel geometry and counting';
-- Grouping by a Geometry (a Variant) requires opting in, because grouping by Variant is restricted by default.
SELECT geom, count() AS cluster_count
FROM (SELECT mvtEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom FROM mvt_points)
GROUP BY geom
ORDER BY geom
SETTINGS allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 1;

SELECT '-- mvtEncode: a shared attribute value is interned once, so a tile with one distinct value is smaller';
-- The tile is compared by length rather than by exact bytes because mvtEncode is order-dependent: the order
-- of the feature messages (and hence the exact bytes) depends on the order rows reach the aggregate, but the
-- total length does not. A shared value yields a one-entry value pool, so the tile is shorter than when the
-- two features carry distinct values.
SELECT
    length(mvtEncode('points')(geom, tuple(toUInt64(5))::Tuple(cluster_count UInt64)))
        < length(mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)))
FROM
(
    SELECT (10.0, 20.0)::Point::Geometry AS geom, toUInt64(5) AS cluster_count
    UNION ALL
    SELECT (30.0, 40.0)::Point::Geometry AS geom, toUInt64(6) AS cluster_count
);

SELECT '-- mvtEncode: each supported property type maps to its vector tile value type';
SELECT hex(mvtEncode('t')(
    (0.0, 0.0)::Point::Geometry,
    tuple('s', toFloat32(1.5), toFloat64(2.5), toInt32(-7), toUInt32(8), toDate('2020-01-01'), toDate32('2020-01-01'), true)
        ::Tuple(str String, f32 Float32, f64 Float64, i Int32, u UInt32, d Date, d32 Date32, b Bool)));

SELECT '-- mvtEncode: a NULL property omits that attribute for the feature';
SELECT hex(mvtEncode('t')(
    (0.0, 0.0)::Point::Geometry,
    tuple(CAST(NULL, 'Nullable(String)'), toUInt32(8))::Tuple(str Nullable(String), u UInt32)));

SELECT '-- mvtEncode: LowCardinality property types are accepted';
SELECT hex(mvtEncode('t')((0.0, 0.0)::Point::Geometry, tuple('x')::Tuple(str LowCardinality(String))));

SELECT '-- mvtEncode: the -State / -Merge combinators round-trip the aggregate state to the same tile';
SELECT hex(mvtEncodeMerge('points')(s))
FROM
(
    SELECT mvtEncodeState('points')((124.0, 3384.0)::Point::Geometry, tuple(toUInt64(3))::Tuple(cluster_count UInt64)) AS s
);

SELECT '-- mvtEncode: the geometry argument must be of type Geometry';
SELECT mvtEncode('t')(tuple(0.0, 0.0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- mvtEncode: a non-named properties tuple is rejected';
SELECT mvtEncode('t')((0.0, 0.0)::Point::Geometry, tuple(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- mvtEncode: an unsupported property type is rejected';
SELECT mvtEncode('t')((0.0, 0.0)::Point::Geometry, tuple([1, 2])::Tuple(arr Array(UInt8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- mvtEncode: the layer name parameter is required';
SELECT mvtEncode((0.0, 0.0)::Point::Geometry); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE mvt_points;
